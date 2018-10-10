'use strict'; // eslint-disable-line strict

const stream = require('stream');


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
    chunkedStreamDemux,
};
