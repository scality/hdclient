'use strict'; // eslint-disable-line strict

const assert = require('assert');

const protocol = require('./protocol');
const httpUtils = require('./http_utils');
const utils = require('./utils');

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
    assert.ok(nChunks === 1 || size > nChunks * chunkSize);

    /* Shortcut not chunked stream */
    if (nChunks === 1) {
        chunkCallback(inputStream, size, 0, callbackArgs);
        return;
    }

    /* TODO: activate and test when doing split
     don't forget to import stream

    let readSize = 0;
    let chunkId = 0;
    let nextBoundary = chunkSize;
    let chunkStream = new stream.PassThrough();

    // Force input to stand still untill event handlers are setup
    inputStream.pause();

    // Setup chunking
    inputStream.on('data', chunk => {
        let leftover = chunk;
        do {
            const pushSize = readSize + leftover.length <= nextBoundary ?
                      chunk.length : nextBoundary - readSize;
            chunkStream.push(leftover.slice(0, pushSize));
            leftover = leftover.slice(pushSize);
            readSize += pushSize;

            // We must switch streams
            if (readSize === nextBoundary) {
                ++chunkId;
                nextBoundary = chunkId === nChunks ?
                    size : (chunkId + 1) * chunkSize;

                chunkStream.push(null); // terminates current stream
                chunkStream = new stream.PassThrough();

                // Kick start callback on next chunk
                chunkCallback(chunkStream, nextBoundary - readSize,
                              chunkId, callbackArgs);
            }
        } while(readSize + leftover.length > nextBoundary);
    });

    // Propagate error
    inputStream.on('error', err =>
                   chunkStream.emit('error', err));

    // Kick-start everything
    inputStream.resume();
    chunkCallback(chunkStream, nextBoundary - readSize,
                  chunkId, callbackArgs);
     */
}

/**
 * Demultiplex a stream for replication
 *
 * Helper to move from single output to
 * multiple identical output streams
 *
 * @param {stream.Readable} inputStream - Stream to chunk
 * @param {Number} size - Total stream size
 * @param {Number} nReplica - Number of output stream
 * @param {function} replicaCallback - Invoked on each replica stream
 *                    (repStream, size, repId, callbackArgs) -> undefined
 * @param {Object} callbackArgs - Arguments to pass when invoking callback
 * @return {undefined}
 */
function replicationStreamDemux(
    inputStream, size, nReplica, replicaCallback, callbackArgs) {
    utils.range(nReplica).forEach(ridx => {
        replicaCallback(inputStream, size, ridx, callbackArgs);
    });
}

/**
 * Decide what is the final status of the operation
 * and what to do on error fragments.
 *
 * @param {Object} opContext - Operation context
 * @return {Error|null} error - what to feed to client callback
 * @return {Object|null} errorHandler - Object tracking cleanup action
 */
function decidePUT(opContext) {
    /*
     * Failure to put any fragment should fail
     * the overall operation, as specified in
     * the specs (note this is still under discussion)
     *
     * Timeout ie unknown status is not considered
     * a failure per se. Chunk might still be queud
     * inside the hyperdrive. We must however check
     * it asap. If too many fragments are in limbo,
     * we must however fail the PUT, to be on the
     * safe side.
     */
    const perChunkMinValid = opContext.fragments.code === 'CP' ?
              Math.floor(opContext.fragments.nDataParts / 2) :
              opContext.fragments.nDataParts;

    const success = opContext.status.every(
        chunk => (chunk.nOk > perChunkMinValid && chunk.nError === 0));

    /* If PUT is successful, notify every timeout fragment
     * for async checks:repair.
     * If it is not, we must delete every potentially written
     * fragment so as not to leave orphans behind. This is
     * done asynchronously, as when failing to delete a fragment.
     */
    const toDelete = [];
    const toCheck = [];
    let worstError = null;
    opContext.status.forEach((chunk, chunkId) => {
        chunk.statuses.forEach((status, fragmentId) => {
            if (!success) { // Async delete everything
                toDelete.push([chunkId, fragmentId]);
            } else if (status.timeout) {
                toCheck.push([chunkId, fragmentId]);
            }

            if (utils.compareErrors(status.error, worstError) > 0) {
                worstError = status.error;
            }
        });
    });

    if (success) {
        // Add fragments to be checks
        const errorHandlers = {};
        if (toCheck.length > 0) {
            errorHandlers.check = {
                fragments: toCheck,
                rawKey: opContext.rawKey,
            };
        }
        return { error: null, errorHandlers };
    }

    const error = worstError;
    const errorHandlers = {
        delete: {
            fragments: toDelete,
            rawKey: opContext.rawKey,
        },
    };

    return { error, errorHandlers };
}

/**
 * Persists detected sub-queries errors for later clean up
 *
 * @param {Object} opContext - Operation context
 * @param {Object} errorAgent - Agent to persist errors
 * @param {Object} errorObj - Description of errors to be persisted
 * @param {HyperdriveClient~deleteCallback} callback - Callback
 * @param {null|Error} currentError - current final status of operation
 * @return {Any} whatever callback returns
 */
function _handleErrors(opContext, errorAgent, errorObj,
                       callback, currentError) {
    const payloads = [];

    if (errorObj.delete) {
        payloads.push({
            topic: httpUtils.topics.delete,
            key: opContext.fragments.objectKey,
            messages: [JSON.stringify(errorObj.delete)],
        });
    }

    if (errorObj.check) {
        payloads.push({
            topic: httpUtils.topics.check,
            key: opContext.fragments.objectKey,
            messages: [JSON.stringify(errorObj.check)],
        });
    }

    errorAgent.send(payloads, err => {
        if (err) {
            /* Return persistence error */
            // TODO: not sure this a proper HTTP error....
            // TODO: must be a better way than disabling it
            // eslint-disable-next-line no-param-reassign
            err.infos = { status: 500, method: 'PUT' };
            opContext.log.error(
                'Failure to persist bad fragments fragments', err);
            return callback(err, opContext.rawKey);
        }

        /* Error persistence OK, return already encountered
         * error (might be null) */
        return callback(currentError, opContext.rawKey);
    });
}

/**
 * PUT a single fragment
 *
 * @param {stream.Readable} inputStream - Data to send
 * @param {Nulber} size - Size of inputStream
 * @param {Number} fragmentId - Position in fragment list
 * @param {Object} args - Bundle of various stuff
 * @return {undefined}
 */
function fragmentPUT(inputStream, size, fragmentId, args) {
    const { opContext, chunkId, httpAgent, errorAgent,
            callback, requestTimeoutMs } = args;
    const isData = fragmentId < opContext.fragments.nDataParts;
    const chunk = opContext.fragments.chunks[chunkId];
    const fragment = isData ? chunk.data[fragmentId] :
              chunk.coding[fragmentId - opContext.fragments.nDataParts];

    const { hostname, port, key } = fragment;
    const contentType = protocol.helpers.makePutContentType(
        { data: size } /* Only 'data' payload is supported */
    );
    const requestOptions = httpUtils.getCommonStoreRequestOptions(
        httpAgent, hostname, port, key);

    requestOptions.method = 'PUT';
    requestOptions.headers = {
        ['Content-Length']: size,
        ['Content-Type']: contentType,
    };

    const reqContext = { opContext, chunkId, fragmentId };
    const request = httpUtils.newRequest(
        requestOptions, opContext.log, reqContext, requestTimeoutMs,
        /* callback */
        reqCtx => {
            const opCtx = reqCtx.opContext;
            /* Wait for all to answer */
            if (opCtx.nPending !== 0) {
                return null;
            }

            let ret = null;
            const { error, errorHandlers } = decidePUT(opCtx);
            if (errorHandlers.delete || errorHandlers.check) {
                ret = _handleErrors(opCtx, errorAgent,
                                    errorHandlers,
                                    callback, error);
            } else {
                ret = callback(error, opCtx.rawKey);
            }
            opCtx.log.end().debug('End PUT');
            return ret;
        });

    /* Plug and propagate errors */
    inputStream.pipe(request);
    inputStream.on('error', err => request.emit('error', err));
}

/**
 * Put all fragments of an object
 *
 * @param {http.HttpAgent} httpAgent - Agent to use
 * @param {Object} errorAgent - Agent to persist errors
 * @param {werelogs.Logger} logger - Logger to use
 * @param {Object} fragments - Object description to delete
 * @param {String} rawKey - Uri of the object
 *                 (refer to keyscheme.js for content)
 * @param {HyperdriveClient~putCallback} callback - Callback
 * @param {Number} requestTimeoutMs - Timeout of each sub-query
 * @param {Number} size - Stream length
 * @param {stream.Readable} inputStream - Stream to store
 * @returns {Object} Operation context tracking everything
 */
function doPUT({ httpAgent, errorAgent, log, fragments, rawKey,
                 callback, requestTimeoutMs, size, inputStream }) {
    // Split, replication or erasure coding is currently not supported
    assert.strictEqual(fragments.nChunks, 1);
    assert.strictEqual(fragments.nCodingParts, 0);

    const opContext = httpUtils.makeOperationContext(fragments, rawKey, log);

    /* TODO: split (see chunkedStreamDemux & erasure coding */

    /* Dispatch replication */
    chunkedStreamDemux(
        inputStream, size, fragments.nChunks, fragments.splitSize,
        (chunkStream, chunkSize, chunkId) =>
            replicationStreamDemux(
                chunkStream, chunkSize, opContext.fragments.nDataParts,
                fragmentPUT,
                { opContext, chunkId, httpAgent, errorAgent,
                  callback, requestTimeoutMs })
        ,
        opContext);

    return opContext;
}

module.exports = {
    doPUT,
};
