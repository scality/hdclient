'use strict'; // eslint-disable-line strict

/**
 * regroup various helper code
 */


/**
 * Format an error into an Arsenal-compatible error object
 *
 * @param {String} type - Type of the error (must not contain spaces)
 * @param {Number} code - Error code, very often a HTTP code
 * @param {String} description - Error message to display
 * @param {Object} additionalInfos - Any object to attach to the error
 *
 * @return {Error} A new Error object with addtional fields
 *                 (code, description, type and infos)
 */
function mockedArsenalError(type, code, description, additionalInfos) {
    const mockedError = new Error(type);
    mockedError.infos = additionalInfos;
    mockedError.code = code;
    mockedError.description = description;
    mockedError[type] = true;
    return mockedError;
}

/**
 * Compare two HTTP errors
 *
 * @param {null|undefined|Error} lhs - Left hand side
 * @param {null|undefined|Error} rhs - Right hand side
 * @return {Number} ~ lhs - rhs
*/
function compareErrors(lhs, rhs) {
    const noLhs = (lhs === undefined || lhs === null);
    const noRhs = (rhs === undefined || rhs === null);

    if (noLhs && noRhs) {
        return 0;
    } else if (noRhs) {
        return 1;
    } else if (noLhs) {
        return -1;
    }

    return lhs.code - rhs.code;
}

/**
 * Get an array filled with [0, n[
 *
 * @param {Number} n - Range upper end (exclusive)
 * @return {Number[]} range array
 */
function range(n) {
    /* Javascript... */
    return [...Array(n).keys()];
}

/**
 * Extract the range to GET inide a chunk, gival object-wide range
 *
 * @param {Objects} fragments - Parsed/generated key
 * @param {Number} chunkId - Which chunk are we analyzing
 * @param {null|Number[]} globalRange - object-level GET request parameter
 * @return {Object} key with {use: Boolean, range: [Number, Number]| undefined}
 * @comment If use if false, we don't have to retrieve this chunk
 */
function getChunkRange(fragments, chunkId, globalRange) {
    if (globalRange === null ||
        globalRange === undefined ||
        fragments.nChunks === 1) {
        return { use: true, chunkRange: globalRange }; // All
    }

    const splitSize = fragments.splitSize;
    const start = chunkId * splitSize;
    const end = Math.min(fragments.size,
                         (chunkId + 1) * splitSize - 1);

    const gstart = Math.max(globalRange[0], 0);
    const gend = globalRange.length === 2 ?
              globalRange[1] : fragments.size;

    if (end <= gstart) {
        /* Start is after the range */
        return { use: false };
    } else if (gend < start) {
        /* Requested ends before this chunk */
        return { use: false };
    }

    const cstart = Math.max(start, gstart);
    const cend = Math.min(end, gend);
    return { use: true, chunkRange: [cstart - start, cend - start] };
}

/**
 * Resolve UUID into host:port
 *
 * @param {Object} uuidmapping - Map UUIDS to hyperdrive endpoints (ip:port)
 * @param {String} uuid - UUID to resolve
 * @return {null|Object} null on error, {hostname, port} on success
 */
function resolveUUID(uuidmapping, uuid) {
    return uuidmapping[uuid];
}

/**
 * Shallow copy an array
 *
 * @param {Object[]} array to copy
 * @return {Object[]} a shallow copy
 *
 * No need for the deep version for now as it is
 * used on Number[] where shallow is same as deep.
 */
function copyArray(array) {
    return [...array];
}

/**
 * Draw a sample from the specified Categorical distribution
 *
 * @param {Number[]} weights of the categories
 * @param {Number|null} normalizationConstant if non-null,
 *                      avoids O(n) renormalization
 * @return {Number} Sampled category (integer in [0, weights.length[
 * @comment Every weights is assumed to be >= 0.
 * @comment Normalization constant should be > 0, or computed as such
 * @comment Normalization is optional. This is an optimization since
 *          hdclient will track and update it as required. Avoids
 *          constantly looping over.
 */
function categoricalSample(weights, normalizationConstant = null) {
    const sum = normalizationConstant || weights.reduce((a, b) => a + b, 0);
    const uSample = Math.random() * sum;
    for (let acc = 0.0, i = 0; i < weights.length; i++) {
        acc += weights[i];
        if (uSample <= acc) {
            return i;
        }
    }
    return null;
}


module.exports = {
    mockedArsenalError,
    compareErrors,
    getChunkRange,
    range,
    resolveUUID,
    copyArray,
    categoricalSample,
};
