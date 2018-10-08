'use strict'; // eslint-disable-line strict

const assert = require('assert');
const stream = require('stream');
const ecstream = require('ecstream');

const protocol = require('./protocol');
const httpUtils = require('./http_utils');
const utils = require('./utils');
const { CorruptSafeStream } = require('./corruption_stream_checker');


function _isReplyBad(status) {
    if (!status.error || !status.error.code) {
        return false;
    }

    const httpCode = status.error.code;
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
        version: 1,
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
            err => {
                // eslint-disable-next-line no-param-reassign
                opContext.failedToPersist = true;
                const enhancedError = utils.mockedArsenalError(
                    'InternalError',
                    500,
                    `Failed to persist fragments to repair: ${err.message}`);
                opContext.log.error(enhancedError);
                errorHandle(enhancedError);
            });
}


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


/**
 * Replication -  Callback of fragment GET query
 *
 * @param {Object} reqContext - Fragment erquest context
 * @param {Function} callback - Chunk GET callback: reply -> ?
 * @param {Object} errorAgent - Agent to persist errors
 * @return {Object} Whatever callback returns
 */
function _replicationGETCb(reqContext, callback, errorAgent) {
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

    /* We are the first one successful of the chunk, callback client */
    if (chunkStatus.nOk === 1 && status.response) {
        const saferStream = getCorruptionCheckedStream(
            status.response, reqContext, errorAgent);
        ret = callback(saferStream);
    } else if (chunkStatus.nOk > 1 && status.response) {
        /* Unused received input streams must be consumed */
        status.response.resume();
    }

    /* Wait for all fragments in the chunk to answer */
    if (nReceivedChunks !== nSendChunks) {
        return ret;
    }

    /* Nothing good so far, we must call upper layer wih no answer
     * Persist errors (like 404, corrupted or else) */
    if (chunkStatus.nOk === 0) {
        callback(null);
    }

    return ret;
}

/**
 * Erasure coding -  Callback of fragment GET query
 *
 * @param {Object} reqContext - Fragment erquest context
 * @param {Function} callback - Chunk GET callback: reply -> ?
 * @param {Object} errorAgent - Agent to persist errors
 * @return {Object} Whatever callback returns
 */
function _erasureGETCb(reqContext, callback, errorAgent) {
    let ret = null;
    const opContext = reqContext.opContext;
    const k = opContext.fragments.nDataParts;

    const chunkStatus = opContext.status[reqContext.chunkId];
    const status = chunkStatus.statuses[reqContext.fragmentId];
    const nSendChunks = opContext.fragments.nDataParts +
              opContext.fragments.nCodingParts;
    const nReceivedChunks = chunkStatus.nOk +
              chunkStatus.nError + chunkStatus.nTimeout;

    /* Good fragment received, wrap in corruption checker */
    if (status.response) {
        status.response = getCorruptionCheckedStream(
            status.response, reqContext, errorAgent);
    }

    /* We received the kth good fragment, we can decode thne callback client
     * NB: decode must be called only once!
     */
    // TODO: if not only data fragment received, delay X ms to
    // potentially receive the missing data parts and avoid decoding?
    if (chunkStatus.nOk === k && status.response) {
        /* Use as much data stream as possible */
        const lastChunk = (reqContext.chunkId + 1
                           === opContext.fragments.nChunks);
        const chunkSize = lastChunk ?
                  (opContext.fragments.size -
                   opContext.fragments.splitSize * reqContext.chunkId) :
                  opContext.fragments.splitSize;
        const istreams = chunkStatus.statuses.map(st => st.response);
        const decodedStream = new stream.PassThrough();
        ecstream.decode(
            decodedStream,
            chunkSize,
            istreams.slice(0, k),
            istreams.slice(k),
            opContext.fragments.stripeSize);
        ret = callback(decodedStream);
    } else if (chunkStatus.nOk > k && status.response) {
        /* Unused received input streams must be consumed */
        status.response.resume();
    }

    /* Wait for all fragments in the chunk to answer
     * Note: we could reply as soon as we know we
     * have lost/not retrieved more than m fragments
     * but simpler to wait for all and possibly
     * persist all errors.
     */
    if (nReceivedChunks !== nSendChunks) {
        return ret;
    }

    /* Not enough good fragments, we must call upper layer wih no answer
     * Persist errors (like 404, corrupted or else) */
    if (chunkStatus.nOk < k) {
        callback(null);
    }

    return ret;
}

/**
 * Callback of fragment GET query
 *
 * @param {Object} reqContext - Fragment erquest context
 * @param {Function} callback - Chunk GET callback: reply -> ?
 * @param {Object} errorAgent - Agent to persist errors
 * @return {Object} Whatever callback returns
 */
function _fragmentGETCb(reqContext, callback, errorAgent) {
    switch (reqContext.opContext.fragments.code) {
    case 'CP':
        return _replicationGETCb(reqContext, callback, errorAgent);
    case 'RS':
        return _erasureGETCb(reqContext, callback, errorAgent);
    default:
        throw new Error(
            `Unknown erasure code: ${reqContext.opContext.fragments.code}`);
    }
}

/**
 * GET a single fragment
 *
 * @param {http.HttpAgent} httpAgent - Agent to use
 * @param {Object} errorAgent - Agent to persist errors
 * @param {Object} opContext - Operation context
 * @param {Function} callback - Chunk GET callback: reply -> ?
 * @param {Number} chunkId - Current chunk number
 * @param {Number} fragmenId - Current fragment number
 * @param {Number} requestTimeoutMs - Timeout of each sub-query
 * @param {null|[Number]} range - HTTP range requested
 * @param {Object} uuidmapping Map UUIDS to hyperdrive endpoints (ip:port)
 * @return {undefined}
 */
function fragmentGET(
    { httpAgent, errorAgent, opContext, callback,
      chunkId, fragmentId, requestTimeoutMs, range, uuidmapping, args }) {
    const isData = fragmentId < opContext.fragments.nDataParts;
    const chunk = opContext.fragments.chunks[chunkId];
    const fragment = isData ? chunk.data[fragmentId] :
              chunk.coding[fragmentId - opContext.fragments.nDataParts];

    const reqContext = { opContext, chunkId, fragmentId, args };
    const { uuid, key } = fragment;
    const { hostname, port } = utils.resolveUUID(uuidmapping, uuid);
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


/**
 * Examine all the statuses, select worst error
 * and action (persisting the errors).
 *
 * @param {Object} opContext - Operation context
 * @param {Object} errorAgent - Agent to persist errors
 * @param {HyperdriveClient~getCallback} clientCb - Callback
 * @param {boolean} callClientCb - Invoke callback if erros are detected
 * @param {boolean} force - Invoke callback even when no errors need
 *                          to be persisted
 * @return {null|Error} selected worst error
 */
function processChunkStatuses(
    opContext, errorAgent, clientCb, callClientCb, force) {
    const { error, errorHandler } = decideGET(opContext);
    if (errorHandler.fragments.length !== 0) {
        _handleErrors(
            opContext, errorAgent, errorHandler,
            clientCb, error, callClientCb);
    } else if (force) {
        clientCb(error, null);
    }
    return error;
}


/**
 * Forward chunk data/error to upper layer
 *
 * Invoked once a single successfull fragment
 * reply is received or when all failed in the chunk
 *
 * @param {Number} chunkId - Chunk to process
 * @param {null|stream.Readable} reply - Returned chunk reply
 * @param {stream.Readable} multiplexedStream - Aggregated output stream
 * @param {Object} args - Various bundle of necessary
 *                        dependencies (see call site)
 * @return {undefined}
 */
function _chunkGETCb(chunkId, reply, multiplexedStream, args) {
    const opContext = args.opContext;
    if (!reply) {
        const error = processChunkStatuses(
            opContext, args.errorAgent, args.callback,
            !opContext.firstProcessedChunk, // Call cb on detected errors?
            !opContext.firstProcessedChunk); // Force, call cb anyway
        // Streaming started, propagate error via stream
        if (opContext.firstProcessedChunk) {
            multiplexedStream.emit('error', error);
        }
        return;
    }

    /* First chunk has replied, start streaming to client */
    if (!opContext.firstProcessedChunk) {
        opContext.firstProcessedChunk = true;
        args.callback(null, multiplexedStream);
        multiplexedStream.resume();
    }

    /* Pipe received stuff or propagate error */
    /* Handle 'end' in a manual way */
    const maskEnd = (chunkId + 1 < opContext.fragments.nChunks);
    reply.pipe(multiplexedStream, { end: !maskEnd });
    reply.on('error', () => {
        const error = processChunkStatuses(
            opContext, args.errorAgent, args.callback, false, false);
        multiplexedStream.emit('error', error);
    });

    /* Request next chunk when current one is consumed*/
    // eslint-disable-next-line no-use-before-define
    reply.once('end', () => chunkGET(
        multiplexedStream, chunkId + 1, args));
}


/**
 * Retrieve all chunk of an object, starting with chunkId
 *
 * @param {stream.Readable} multiplexedStream - Aggregated output stream
 * @param {Number} chunkId - Chunk to process
 * @param {Object} baseFragmentGETArgs - Various bundle of necessary
 *                                       dependencies (see call site)
 * @return {undefined}
 * @comment All chunks are retrieved sequentially
 */
function chunkGET(multiplexedStream, chunkId, baseFragmentGETArgs) {
    const opContext = baseFragmentGETArgs.opContext;
    /* Handle last chunk + 1, called after last chunk
     * is fully processed */
    if (chunkId === opContext.fragments.nChunks) {
        processChunkStatuses(
            opContext,
            baseFragmentGETArgs.errorAgent,
            baseFragmentGETArgs.callback,
            !opContext.firstProcessedChunk,
            false);

        opContext.log.end().debug('End GET');
        return;
    }

    const chunk = opContext.fragments.chunks[chunkId];

    /* Identify whether we need to retrieve this chunk */
    const { use, chunkRange } = utils.getChunkRange(
        opContext.fragments, chunkId, baseFragmentGETArgs.range);
    if (!use) {
        /* Consider chunk as done */
        opContext.nPending -= opContext.fragments.nDataParts +
            opContext.fragments.nCodingParts;
        /* Retrieve next one */
        setImmediate(() => chunkGET(
            multiplexedStream, chunkId + 1, baseFragmentGETArgs));
        return;
    }

    /* Dispatch GET query to all fragments of the chunk */
    [...chunk.data, ...chunk.coding].forEach((fragment, fragmentId) =>
        fragmentGET({
            opContext,
            fragmentId,
            chunkId,
            range: chunkRange,
            requestTimeoutMs: baseFragmentGETArgs.requestTimeoutMs,
            httpAgent: baseFragmentGETArgs.httpAgent,
            errorAgent: baseFragmentGETArgs.errorAgent,
            uuidmapping: baseFragmentGETArgs.uuidmapping,
            callback: reply => _chunkGETCb(
                chunkId, reply, multiplexedStream, baseFragmentGETArgs),
        })
    );
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
 * @param {Object} uuidmapping Map UUIDS to hyperdrive endpoints (ip:port)
 * @returns {Object} Operation context tracking everything
 */
function doGET({ httpAgent, errorAgent, log, fragments, rawKey,
                 callback, requestTimeoutMs, range, uuidmapping }) {
    const opContext = httpUtils.makeOperationContext(fragments, rawKey, log);
    if (range && range[0] && fragments.size < range[0]) {
        const invRange = Error(`Invalid range: ${range}`);
        invRange.infos = { status: 406, method: 'GET' };
        callback(invRange, null);
        return opContext;
    }

    const multiplexedStream = new stream.PassThrough();
    const baseFragmentGETArgs = {
        httpAgent, errorAgent, opContext, callback,
        requestTimeoutMs, range, uuidmapping };
    chunkGET(multiplexedStream, 0 /* starting chunk */, baseFragmentGETArgs);

    return opContext;
}

module.exports = {
    doGET,
};
