'use strict'; // eslint-disable-line strict

// Hyperdrive align stored data on 4096 bytes multiples
const DATA_ALIGN = 4096;

function align(k, n) {
    return Math.ceil(k / n) * n;
}

/**
 * Compute split size to use for replicated object
 *
 * @param {Number} minSplitSize Minimum size of a chunk
 * @param {Number} objectSize Total size of object to split
 * @returns {Object} split details, keyed with nChunks, splitSize and stripeSize
 */
function getReplicatedSplitSize(minSplitSize, objectSize) {
    /* Replication storage overhead
     *
     * Only source of overhead is the tail of stream/object,
     * since we _always_ select a splitSize aligned with
     * hyperdrive's extent
     *
     * overhead = (objectSize - align(objectSize, DATA_ALIGN))
     *                  * nDataParts / DATA_ALIGN
     *
     */
    const alignedSplitSize = align(minSplitSize, DATA_ALIGN);
    if (minSplitSize <= 0 || objectSize <= alignedSplitSize) {
        // No need to split
        return { nChunks: 1, splitSize: objectSize, stripeSize: 0 };
    }

    return {
        nChunks: Math.ceil(objectSize / alignedSplitSize),
        splitSize: alignedSplitSize,
        stripeSize: 0,
    };
}

/**
 * Compute split size to use for erasure coded object
 *
 * @param {Number} minSplitSize Minimum size of a chunk
 * @param {Number} objectSize Total size of object to split
 * @param {Number} nDataParts Number of data endpoints to select
 * @returns {Object} split details, keyed with nChunks, splitSize and stripeSize
 */
function getErasureCodedSplitSize(minSplitSize, objectSize, nDataParts) {
    /* Erasure coding overhead
     *
     * Split size should _always_ be aligned so that each data part is aligned
     * with hyperdrive's extent. Only source of overhead is the last stripe
     * of the last chunk.
     *
     * overhead = (objectSize - align(objectSize, nDataParts * DATA_ALIGN))
     *                  / (nDataParts * DATA_ALIGN)
     */
    const alignedSplitSize = align(minSplitSize, nDataParts * DATA_ALIGN);
    if (minSplitSize <= 0 || objectSize <= alignedSplitSize) {
        // No need to split
        const stripeSize = DATA_ALIGN;
        return { stripeSize, nChunks: 1, splitSize: objectSize };
    }

    return {
        nChunks: Math.ceil(objectSize / alignedSplitSize),
        splitSize: alignedSplitSize,
        stripeSize: DATA_ALIGN,
    };
}

/**
 * Compute split size to use
 *
 * @param {Number} minSplitSize Minimum size of a chunk
 * @param {Number} objectSize Total size of object to split
 * @param {String} code to use (RS or CP for replication - copy)
 * @param {Number} nDataParts Number of data endpoints to select
 * @returns {Object} split details, keyed with nChunks, splitSize and stripeSize
 */
function getSplitSize(minSplitSize, objectSize, code, nDataParts) {
    if (code === 'CP') {
        return getReplicatedSplitSize(minSplitSize, objectSize);
    }

    return getErasureCodedSplitSize(minSplitSize, objectSize, nDataParts);
}

/**
 * Identify which chunks are concerned by the range query
 *
 * @param {Object} fragments - Deserialized raw key
 * @param {null|[Number]} range - HTTP range requested
 * @return {[Object]} Selected slice over fragments.chunks array
 */
function getChunkSlice(fragments, range) {
    if (!range || fragments.nChunks === 1) {
        return fragments.chunks;
    }

    const startChunk = range[0] === null ?
              0 : Math.floor(range[0] / fragments.splitSize);
    const endChunk = range.length < 2 || range[1] === null ?
              fragments.nChunks : Math.ceil(range[1] / fragments.splitSize);
    return fragments.chunks.slice(startChunk, endChunk);
}

module.exports = {
    DATA_ALIGN,
    align,
    getSplitSize,
    getChunkSlice,
};
