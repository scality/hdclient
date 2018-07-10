'use strict'; // eslint-disable-line strict

/**
 * Compute split size to use
 *
 * @param {Number} minSplitSize Minimum size of a chunk
 * @param {Number} objectSize Total size of object to split
 * @returns {Object} split details, keyed with nChunks and splitSize
 */
/* eslint-disable no-unused-vars */
function getSplitSize(minSplitSize, objectSize) {
    /* TODO: split currently not supported */
    return {
        nChunks: 1,
        splitSize: 0,
    };
}
/* eslint-enable no-unused-vars */

module.exports = {
    getSplitSize,
};
