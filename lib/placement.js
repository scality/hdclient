'use strict'; // eslint-disable-line strict

/**
 * Data placement tool
 */

/**
 * Select fragment locations according to policy
 *
 * @param {Object} policy placement
 * @param {Number} nData Number of data fragment
 * @param {Number} nCoding Number of coding fragment
 * @returns {Object} with dataLocations & codingLocations keys
 */
function select(policy, nData, nCoding) {
    /* Simple implementation: pick 1 according to uniform sampling
     * over |endpoints|, and the nDataParts + nCodingParts following.
     * Guarantees each part ends up on a diffrent hyperdrive
     */
    const len = policy.locations.length;
    let pos = Math.floor(Math.random() * len);

    const dataLocations = [];
    for (let i = 0; i < nData; ++i) {
        dataLocations.push(policy.locations[pos]);
        pos = (pos + 1) % len;
    }

    const codingLocations = [];
    for (let i = 0; i < nCoding; ++i) {
        codingLocations.push(policy.locations[pos]);
        pos = (pos + 1) % len;
    }

    return { dataLocations, codingLocations };
}

module.exports = {
    select,
};

