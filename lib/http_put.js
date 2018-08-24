'use strict'; // eslint-disable-line strict

const ecstream = require('ecstream');

const protocol = require('./protocol');
const httpUtils = require('./http_utils');
const utils = require('./utils');
const { chunkedStreamDemux } = require('./stream_chunk');

/**
 * Demultiplex a stream for replication
 *
 * Helper to move from single output to
 * multiple identical output streams
 *
 * @param {stream.Readable} inputStream - Stream to chunk
 * @param {Number} size - Total stream size
 * @param {Object} opContext - Operation context
 * @param {function} fragmentCallback - Invoked on each replica stream
 *                    (size, repId, callbackArgs) -> http.clientRequest
 * @param {Object} callbackArgs - Arguments to pass when invoking callback
 * @return {undefined}
 */
function replicationStreamDemux(
    inputStream, size, opContext, fragmentCallback, callbackArgs) {
    utils.range(opContext.fragments.nDataParts).forEach(ridx => {
        const ostream = fragmentCallback(size, ridx, callbackArgs);
        /* Plug and propagate errors */
        inputStream.pipe(ostream);
        inputStream.on('error', err => ostream.emit('error', err));
    });
}

/**
 * Demultiplex a stream for erasure coding
 *
 * @param {stream.Readable} inputStream - Stream to chunk
 * @param {Number} size - Chunk stream size
 * @param {Object} opContext - Operation context
 * @param {function} fragmentCallback - Invoked on each replica stream
 *                    (size, repId, callbackArgs) -> http.clientRequest
 * @param {Object} callbackArgs - Arguments to pass when invoking callback
 * @return {undefined}
 */
function erasureStreamDemux(
    inputStream, size, opContext, fragmentCallback, callbackArgs) {
    const k = opContext.fragments.nDataParts;
    const m = opContext.fragments.nCodingParts;
    const stripeSize = opContext.fragments.stripeSize;
    const nStripes = Math.ceil(size / (stripeSize * k));
    const contentSize = nStripes * stripeSize;
    const dataStreams = utils.range(k).map(
        ridx => fragmentCallback(contentSize, ridx, callbackArgs));
    const codingStreams = utils.range(m).map(
        ridx => fragmentCallback(contentSize, k + ridx, callbackArgs));

    ecstream.encode(
        inputStream, size,
        dataStreams, codingStreams,
        stripeSize);
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
                version: 1,
            };
        }
        return { error: null, errorHandlers };
    }

    const error = worstError;
    const errorHandlers = {
        delete: {
            fragments: toDelete,
            rawKey: opContext.rawKey,
            version: 1,
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
 * @return {undefined}
 */
function _handleErrors(opContext, errorAgent, errorObj,
                       callback, currentError) {
    const promises = [];
    if (errorObj.delete) {
        promises.push(errorAgent.produce(
            httpUtils.topics.delete,
            0, // topic partition
            JSON.stringify(errorObj.delete),
            opContext.fragments.objectKey
        ).catch(err => err));
    }

    if (errorObj.check) {
        promises.push(errorAgent.produce(
            httpUtils.topics.check,
            0, // topic partition
            JSON.stringify(errorObj.check),
            opContext.fragments.objectKey
        ).catch(err => err));
    }

    const handlePromiseError = error => {
        /* Return persistence error */
        // TODO: not sure this a proper HTTP error....
        // TODO: must be a better way than disabling it
        /* eslint-disable no-param-reassign */
        error.infos = { status: 500, method: 'GET' };
        opContext.failedToPersist = true;
        /* eslint-enable no-param-reassign */
        opContext.log.error(
            'Failure to persist bad fragments fragments', error);
        callback(error, opContext.rawKey);
    };

    Promise.all(promises).then(
        /* Error persistence OK, return already encountered
         * error (might be null) */
        values => {
            const rejected = values.filter(e => e instanceof Error);
            if (rejected.length > 0) {
                handlePromiseError(rejected[0]);
                return;
            }
            callback(currentError, opContext.rawKey);
        })
        .catch(handlePromiseError);
}

/**
 * PUT a single fragment
 *
 * @param {Nulber} size - Size of inputStream
 * @param {Number} fragmentId - Position in fragment list
 * @param {Object} args - Bundle of various stuff
 * @return {http.ClientRequest} Request to stream on
 */
function fragmentPUT(size, fragmentId, args) {
    const { opContext, chunkId, httpAgent, errorAgent,
            callback, requestTimeoutMs } = args;
    const isData = fragmentId < opContext.fragments.nDataParts;
    const chunk = opContext.fragments.chunks[chunkId];
    const fragment = isData ? chunk.data[fragmentId] :
              chunk.coding[fragmentId - opContext.fragments.nDataParts];

    const { uuid, key } = fragment;
    const { hostname, port } = utils.resolveUUID(args.uuidmapping, uuid);
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

    return request;
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
 * @param {Object} uuidmapping Map UUIDS to hyperdrive endpoints (ip:port)
 * @returns {Object} Operation context tracking everything
 */
function doPUT({ httpAgent, errorAgent, log, fragments, rawKey,
                 callback, requestTimeoutMs, size, inputStream, uuidmapping }) {
    const opContext = httpUtils.makeOperationContext(fragments, rawKey, log);
    let demux = null;
    switch (fragments.code) {
    case 'CP':
        demux = replicationStreamDemux;
        break;
    case 'RS':
        demux = erasureStreamDemux;
        break;
    default:
        throw new Error(`Unknown code ${fragments.code}`);
    }

    /* Dispatch replication */
    chunkedStreamDemux(
        inputStream, size, fragments.nChunks, fragments.splitSize,
        (chunkStream, chunkSize, chunkId) =>
            demux(
                chunkStream, chunkSize, opContext,
                fragmentPUT,
                { opContext, chunkId, httpAgent, errorAgent,
                  callback, requestTimeoutMs, uuidmapping })
        ,
        opContext);

    return opContext;
}

module.exports = {
    doPUT,
};
