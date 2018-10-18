'use strict'; // eslint-disable-line strict

const stream = require('stream');

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

function newChunk(inputStream) {
    const chunkStream = new stream.PassThrough();
    // Propagate error
    inputStream.on('error', err =>
                   chunkStream.emit('error', err));
    return chunkStream;
}

/**
 * Stream chunking demultiplexer
 *
 * Transform a single input readable stream
 * into multiple readable chunked streams
 *
 * @param {stream.Readable} inputStream - Stream to chunk
 * @param {Number} size - Total stream size
 * @param {Number} nChunks - Number of chunks to create
 * @param {Number} chunkSize - Size of each chunk, except the last one
 * @param {function} chunkCallback - Invoked after each chuk creation
 *                    (chunkStream, size, chunkId, callbackArgs) -> undefined
 * @param {Object} callbackArgs - Arguments to pass when invoking callback
 * @return {undefined}
 *
 * The last chunk can be either smaller than chunkSize (most likely case)
 * or larger if caller requestesd so (e.g. to minimize lost space by
 * hyperdrives)
 * Example: size=1000, nChunks=3, chunkSize=300
 *   => chunk1: [0, 300[, chunk2: [300, 600[, chunk3:[600, 1000[
 */
function chunkedStreamDemux(inputStream, size,
                            nChunks, chunkSize,
                            chunkCallback, callbackArgs) {
    /* Shortcut not chunked stream */
    if (nChunks === 1) {
        chunkCallback(inputStream, size, 0, callbackArgs);
        return;
    }

    let readSize = 0;
    let chunkId = 0;
    let nextBoundary = chunkSize;
    let chunkStream = newChunk(inputStream);

    // Setup chunking
    inputStream.on('data', chunk => {
        let read = 0;
        do {
            const leftover = chunk.length - read;
            const pushSize = readSize + leftover <= nextBoundary ?
                      leftover : nextBoundary - readSize;

            chunkStream.write(
                chunk.slice(read, read + pushSize),
                null /* binary encoding */);

            readSize += pushSize;
            read += pushSize;

            // We must switch streams
            if (readSize === nextBoundary && (chunkId + 1) !== nChunks) {
                ++chunkId;
                const lastChunk = (chunkId + 1) === nChunks;
                nextBoundary = lastChunk ?
                    size : (chunkId + 1) * chunkSize;

                chunkStream.end();
                chunkStream = newChunk(inputStream);
                if (lastChunk) {
                    // eslint-disable-next-line no-loop-func
                    inputStream.on('end', () => chunkStream.end());
                }

                // Kick start callback on next chunk
                chunkCallback(chunkStream, nextBoundary - readSize,
                              chunkId, callbackArgs);
            }
        } while (read < chunk.length);
    });

    // Kick-start everything
    inputStream.resume();
    chunkCallback(chunkStream, nextBoundary - readSize,
                  chunkId, callbackArgs);
}


module.exports = {
    DATA_ALIGN,
    align,
    getSplitSize,
    chunkedStreamDemux,
};
