'use strict'; // eslint-disable-line strict

const assert = require('assert');
const stream = require('stream');

const protocol = require('./protocol');
const httpUtils = require('./http_utils');
const utils = require('./utils');
const { CorruptSafeStream } = require('./corruption_stream_checker');


/**
* Wraps reply stream inside a corruption checker
 *
 * @param {http.IncomingMessage} httpReply - Hyperdrive response stream
 * @param {Object} reqContext - Context of the fragment query
 * @param {Object} errorAgent - Error handler
 * @return {stream.Readable} corruption-safe stream, augmented
 *                           HTTP reply status code and headers
 */
function getCorruptionCheckedStream(httpReply, reqContext, errorAgent) {
    const ctypes = protocol.helpers.parseReturnedContentType(
        httpReply.headers['content-type']);
    const size = ctypes.get('data');
    // Those are available iff GET request contained $crc in Accept header
    const expectedCRC = ctypes.get('$crc.data');

    /* If we don't have the necessary information to
     * check for corruption, simple forward */
    if (!size || !expectedCRC) {
        return httpReply;
    }

    const corruptSafeStream = new CorruptSafeStream(
        reqContext, errorAgent, size, expectedCRC);

    /* Forward useful fields - monkey patching HTTP reply */
    corruptSafeStream.statusCode = httpReply.statusCode;
    corruptSafeStream.headers = httpReply.headers;

    /* Forward error */
    httpReply.on('error', err => corruptSafeStream.emit('error', err));

    httpReply.pipe(corruptSafeStream);
    return corruptSafeStream;
}

function _isReplyBad(status) {
    if (!status.error || !status.error.infos) {
        return false;
    }

    const httpCode = status.error.infos.status;
    return httpCode === 404 ||
        httpCode === 422; /* Corrupted */
}

/**
 * Decide what is the final status of the operation
 * and what to do on error fragments.
 *
 * @param {Object} opContext - Operation context
 * @return {Error|null} error - what to feed to client callback
 * @return {Object|null} errorHandler - Object tracking cleanup action
 */
function decideGET(opContext) {
    const toRepair = [];
    let worstError = null;

    opContext.status.forEach((chunk, chunkId) => {
        chunk.statuses.forEach((status, fragmentId) => {
            if (_isReplyBad(status)) {
                toRepair.push([chunkId, fragmentId]);
            }

            if (utils.compareErrors(status.error, worstError) > 0) {
                worstError = status.error;
            }
        });
    });

    const errorHandler = {
        fragments: toRepair,
        rawKey: opContext.rawKey,
    };
    return { errorHandler, error: worstError };
}

/**
 * Persists detected sub-queries errors for later clean up
 *
 * @param {Object} opContext - Operation context
 * @param {Object} errorAgent - Agent to persist errors
 * @param {Object} errorObj - Description of errors to be persisted
 * @param {HyperdriveClient~deleteCallback} callback - Callback
 * @param {null|Error} currentError - current final status of operation
 * @param {boolean} callClientCb - Should we invoke callback?
 * @return {Any} whatever callback returns
 */
function _handleErrors(opContext, errorAgent, errorObj,
                       callback, currentError, callClientCb) {
    const errorHandle = callClientCb ? callback : () => {};
    return errorAgent.produce(
        httpUtils.topics.repair,
        0, // topic partition,
        JSON.stringify(errorObj),
        opContext.fragments.objectKey)
        .then(
            /* Error persistence OK, return already encountered
             * error (might be null) */
            () => errorHandle(currentError))
        .catch(
            // Return persistence error
            // TODO: not sure this a proper HTTP error....
            // TODO: must be a better way than disabling it
            err => {
                /* eslint-disable no-param-reassign */
                err.infos = { status: 500, method: 'GET' };
                opContext.failedToPersist = true;
                /* eslint-enable no-param-reassign */
                opContext.log.error(
                    'Failure to persist fragments to repair', err);
                errorHandle(err);
            });
}

/**
 * Callback of fragment GET query
 *
 * @param {Object} reqContext - Fragment erquest context
 * @param {HyperdriveClient~getCallback} callback - Client GET callback
 * @param {Object} errorAgent - Agent to persist errors
 * @return {Object} Whatever client callback returns
 */
function _fragmentGETCb(reqContext, callback, errorAgent) {
    let ret = null;
    const opContext = reqContext.opContext;

    // TODO: for erasure coding
    assert.strictEqual(opContext.fragments.nCodingParts, 0);
    const chunkStatus = opContext.status[reqContext.chunkId];
    const status = chunkStatus.statuses[reqContext.fragmentId];
    const nSendChunks = opContext.fragments.nDataParts +
              opContext.fragments.nCodingParts;
    const nReceivedChunks = chunkStatus.nOk +
              chunkStatus.nError + chunkStatus.nTimeout;

    /* We are the first one successful, callback client */
    if (chunkStatus.nOk === 1 && status.response) {
        const saferStream = getCorruptionCheckedStream(
            status.response, reqContext, errorAgent);
        ret = callback(saferStream);
    }

    /* Wait for all fragments in the chunk to answer */
    if (nReceivedChunks !== nSendChunks) {
        return ret;
    }

    /* Nothing good so far
     * Persist errors (like 404, corrupted or else) */
    if (chunkStatus.nOk === 0) {
        callback(null);
    }

    return ret;
}

/**
 * GET a single fragment
 *
 * @param {http.HttpAgent} httpAgent - Agent to use
 * @param {Object} errorAgent - Agent to persist errors
 * @param {Object} opContext - Operation context
 * @param {HyperdriveClient~getCallback} callback - Callback
 * @param {Number} chunkId - Current chunk number
 * @param {Number} fragmenId - Current fragment number
 * @param {Number} requestTimeoutMs - Timeout of each sub-query
 * @param {null|[Number]} range - HTTP range requested
 * @return {undefined}
 */
function fragmentGET(
    { httpAgent, errorAgent, opContext, callback,
      chunkId, fragmentId, requestTimeoutMs, range, args }) {
    const isData = fragmentId < opContext.fragments.nDataParts;
    const chunk = opContext.fragments.chunks[chunkId];
    const fragment = isData ? chunk.data[fragmentId] :
              chunk.coding[fragmentId - opContext.fragments.nDataParts];

    const reqContext = { opContext, chunkId, fragmentId, args };
    const { hostname, port, key } = fragment;
    const requestOptions = httpUtils.getCommonStoreRequestOptions(
        httpAgent, hostname, port, key);

    requestOptions.method = 'GET';
    requestOptions.headers = {
        ['Accept']: protocol.helpers.makeAccept(
            ['data', range], ['crc']),
    };

    const request = httpUtils.newRequest(
        requestOptions, opContext.log, reqContext,
        requestTimeoutMs,
        /* callback */
        reqCtx => _fragmentGETCb(reqCtx, callback, errorAgent));

    request.end();
}

function chunkGET(multiplexedStream, opContext, chunkId, baseFragmentGETArgs) {
    if (chunkId === opContext.fragments.nChunks) {
        const { error, errorHandler } = decideGET(opContext);
        if (errorHandler.fragments.length !== 0) {
            _handleErrors(
                opContext,
                baseFragmentGETArgs.errorAgent,
                errorHandler,
                baseFragmentGETArgs.callback,
                error,
                !opContext.firstProcessedChunk);
        }

        opContext.log.end().debug('End GET');
        multiplexedStream.push(null);
        return;
    }

    // Erasure coding is currently not supported
    assert.strictEqual(opContext.fragments.nCodingParts, 0);

    const chunk = opContext.fragments.chunks[chunkId];

    /* Identify whether we need to retrieve this chunk */
    const { use, chunkRange } = utils.getChunkRange(
        opContext.fragments, chunkId, baseFragmentGETArgs.range);
    if (!use) {
        /* Consider chunk as done */
        opContext.nPending -= opContext.fragments.nDataParts +
            opContext.fragments.nCodingParts;
        /* Retrieve next one */
        chunkGET(multiplexedStream, opContext,
                 chunkId + 1, baseFragmentGETArgs);
        return;
    }

    chunk.data.forEach((fragment, fragmentId) => {
        const fragArgs = {
            opContext,
            fragmentId,
            chunkId,
            range: chunkRange,
            requestTimeoutMs: baseFragmentGETArgs.requestTimeoutMs,
            httpAgent: baseFragmentGETArgs.httpAgent,
            errorAgent: baseFragmentGETArgs.errorAgent,
            callback: reply => {
                if (!reply) {
                    const { error, errorHandler } = decideGET(opContext);
                    if (errorHandler.fragments.length !== 0) {
                        _handleErrors(
                            opContext,
                            baseFragmentGETArgs.errorAgent,
                            errorHandler,
                            baseFragmentGETArgs.callback,
                            error,
                            !opContext.firstProcessedChunk);
                    } else if (!opContext.firstProcessedChunk) {
                        baseFragmentGETArgs.callback(error, null);
                    }
                    multiplexedStream.emit('error', error);
                    return;
                }

                /* First chunk has replied, start streaming to client */
                if (!opContext.firstProcessedChunk) {
                    opContext.firstProcessedChunk = true;
                    baseFragmentGETArgs.callback(null, multiplexedStream);
                    multiplexedStream.resume();
                }

                /* Pipe received stuff or propagate error */
                /* Handle 'end' in a manual way */
                reply.pipe(multiplexedStream, { end: false });
                /* Request next chunk when current one is consumed*/
                reply.on('error', err => {
                    const { error, errorHandler } = decideGET(opContext);
                    if (errorHandler.fragments.length !== 0) {
                        _handleErrors(
                            opContext,
                            baseFragmentGETArgs.errorAgent,
                            errorHandler,
                            baseFragmentGETArgs.callback,
                            error,
                            false);
                    }

                    multiplexedStream.emit('error', err);
                });
                reply.once('end', () => chunkGET(
                    multiplexedStream, opContext,
                    chunkId + 1, baseFragmentGETArgs));
            },
        };

        fragmentGET(fragArgs);
    });
}

/**
 * Get all fragments of an object
 *
 * @param {http.HttpAgent} httpAgent - Agent to use
 * @param {Object} errorAgent - Agent to persist errors
 * @param {werelogs.Logger} logger - Logger to use
 * @param {Object} fragments - Object description to delete
 * @param {String} rawKey - Uri of the object
 *                 (refer to keyscheme.js for content)
 * @param {HyperdriveClient~getCallback} callback - Callback
 * @param {Number} requestTimeoutMs - Timeout of each sub-query
 * @param {Number [] | Undefined} range - Range (if any) with
 *                                        first element the start
 *                                        and the second element the end
 * @returns {Object} Operation context tracking everything
 */
function doGET({ httpAgent, errorAgent, log, fragments, rawKey,
                 callback, requestTimeoutMs, range }) {
    const opContext = httpUtils.makeOperationContext(fragments, rawKey, log);
    if (range && range[0] && fragments.size < range[0]) {
        const invRange = Error(`Invalid range: ${range}`);
        invRange.infos = { status: 406, method: 'GET' };
        callback(invRange, null);
        return opContext;
    }

    const multiplexedStream = new stream.PassThrough();
    const baseFragmentGETArgs = {
        httpAgent, errorAgent, opContext, callback, requestTimeoutMs, range };
    chunkGET(multiplexedStream, opContext, 0, baseFragmentGETArgs);

    return opContext;
}

module.exports = {
    doGET,
};
