'use strict'; // eslint-disable-line strict

/**
 * Compute split size to use
 *
 * @param {Number} minSplitSize Minimum size of a chunk
 * @param {Number} objectSize Total size of object to split
 * @returns {Object} split details, keyed with nChunks, splitSize and stripeSize
 */
/* eslint-disable no-unused-vars */
function getSplitSize(minSplitSize, objectSize) {
    /* TODO: split currently not supported */
    return {
        nChunks: 1,
        splitSize: objectSize,
        stripeSize: 0,
    };
}
/* eslint-enable no-unused-vars */

/**
 * Identify which chunks are concerned by the range query
 *
 * @param {Object} fragments - Deserialized raw key
 * @param {null|[Number]} range - HTTP range requested
 * @return {[Object]} Selected slice over fragments.chunks array
 */
/* eslint-disable no-unused-vars */
function getChunkSlice(fragments, range) {
    return fragments.chunks;

    /* TODO: activate for split - yet untested
    if (fragments.splitSize === 0 || range === null) {
        return fragments.chunks;
    }

    const startChunk = range[0] === null ?
              0 : Math.floor(range[0] / fragments.SplitSize);
    const endChunk = range.length < 2 || range[1] === null ?
              fragments.nChunks : Math.ceil(range[1] / fragments.SplitSize);
    return fragments.chunks.slice(startChunk, endChunk);
     */
}
/* eslint-enable no-unused-vars */

module.exports = {
    getSplitSize,
    getChunkSlice,
};
