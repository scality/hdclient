'use strict'; // eslint-disable-line strict

/**
 * Data placement tool
 *
 * Requirements and functional/higher-level details
 * can be found in ../docs/Desgin.md.
 *
 *
 * Glossary:
 * - cluster: root of a tree of component
 * - hyperdrive: leaf of component tree
 *   { name: required
 *     affinity: required but default set by config validation
 *     ftype: required but default set by config validation
 *   }
 * - component: internal nodes of the tree
 *   { name: required but default set by config validation
 *     components: required - children of the node
 *     dynamicWeights: required, filled by config validation
 *     dynamicSum: required, filled by config validation
 *     affinity: required but default set by config validation
 *     ftype: required but default set by config validation
 *   }
 *
 * Sampling multiple fragments requires updating
 * the components weights (affinity='hard' or simply
 * lowering weight to ease fragment spread (see updateWeights).
 * It means we must somehow deep copy the necessary weights to:
 * 1/ properly sample multiple fragments
 * 2/ properly separate concurrent select
 *
 * To avoid a full deep-copy each time select is called,
 * the implementation relies on a so-called 'lazy index'.
 * This index is an associative array mapping a component
 * to a copy of its weights - copy performed the first we
 * cache it (see cacheDistribution). The index is flattened,
 * and nested keys are constructed as '<component.name> <depth> <idx>'.
 *
 * NB: the key triplet should be unique
 *
 * A flattened index, and indeed the existing components store
 * the weights themselves (dynamicWeights) but also their total
 * (dynamicSum). This is an optimization to speed up sampling
 * from the categorical distribution (refer to utils.categoricalSample).
 * An index entry is an object of the form:
 * { weights: Number[], sum: Number > 0 }.
 */

const utils = require('./utils');

/**
 * Cache a copy of the component's dyn_weights
 *
 * @param {Map} lazyIndex to store cached entry
 * @param {String} idx Entry under which to index cached entry
 * @param {Object} component Component to 'cache'
 * @return {undefined}
 */
function cacheDistribution(lazyIndex, idx, component) {
    if (!lazyIndex[idx]) {
        /* eslint-disable no-param-reassign */
        lazyIndex[idx] = {
            weights: utils.copyArray(component.dynamicWeights),
            sum: component.dynamicSum,
        };
        /* eslint-enable no-param-reassign */
    }
}

/**
 * In-place weight update
 *
 * Since we have chosen child #sample of component,
 * slightly lower its weight to help disperse later
 * fragments on its siblings. If component has a
 * 'hard' policy, final weight should be 0 to avoid
 * any further selection.
 *
 * @param {Object} idxEntry Cached entry to update
 * @param {Number} sample Index of selected child
 * @param {String} affinity Affinity of the child - 'soft' or 'hard'
 * @return {undefined}
 */
function updateWeights(idxEntry, sample, affinity) {
    let newWeight = 0.0;
    if (affinity === 'soft') {
        newWeight = idxEntry.weights[sample] * 0.8;
    }
    /* eslint-disable no-param-reassign */
    idxEntry.sum += (newWeight - idxEntry.weights[sample]);
    idxEntry.weights[sample] = newWeight;
    /* eslint-enable no-param-reassign */
}

/**
 * Sample a child of component with a matching ftype
 *
 * @param {Object} component Component to sample
 * @param {Object} idxEntry Cache entry
 * @param {String} ftype Fragment type to match (see comment below)
 * @return {Number} Index/Category jsut sampled, -1 on error
 * @comment Reminder: ftype can be either 'data', coding' or 'both'
 */
function sampleMatchingTypeComponent(component, idxEntry, ftype) {
    let sample = -1;
    let sum = idxEntry.sum;
    const weights = utils.copyArray(idxEntry.weights);
    /* Rejection sampling until we find a matching ftype */
    while (sum > 0) {
        sample = utils.categoricalSample(
            weights, sum);
        const child = component.components[sample];
        if (child.ftype === ftype || child.ftype === 'both') {
            break;
        }
        /* Discard unmatched component from the distribution */
        sum -= weights[sample];
        weights[sample] = 0.0;
    }

    if (sum <= 0) {
        return -1;
    }

    return sample;
}

/**
 * Select a single fragment locations according to policy
 *
 * @param {Object} cluster Topology root
 * @param {Object} lazyIndex Track weights changes due to
 *                           previous fragment samples
 * @param {String} ftype Fragment type 'data' or 'coding'
 * @return {null|String} Hyperdrive uuid on success, null on error
 * @comment no recursion, since nodejs as no TCO
 */
function selectOne(cluster, lazyIndex, ftype) {
    let component = cluster;
    let sample = 0;
    let depth = 0;

    /* Until we reach the deepest level */
    while (component.components) {
        const idx = `${component.name} ${depth} ${sample}`;
        cacheDistribution(lazyIndex, idx, component);
        sample = sampleMatchingTypeComponent(
            component, lazyIndex[idx], ftype);
        if (sample < 0) {
            return null;
        }
        const child = component.components[sample];
        updateWeights(lazyIndex[idx], sample, child.affinity);
        depth += 1;
        component = child;
    }

    return component.name;
}

/**
 * Select fragment locations according to policy
 *
 * @param {Object} policy Placement description (see documentation for format)
 * @param {Number} nData Number of data fragment
 * @param {Number} nCoding Number of coding fragment
 * @returns {Object} with dataLocations & codingLocations keys
 */
function select(policy, nData, nCoding) {
    const lazyWeights = {};
    const locations = utils.range(nData + nCoding).map(
        i => selectOne(policy.cluster, lazyWeights,
                       i < nData ? 'data' : 'coding'));

    return { dataLocations: locations.slice(0, nData),
             codingLocations: locations.slice(nData) };
}

module.exports = {
    sampleMatchingTypeComponent,
    select,
};

