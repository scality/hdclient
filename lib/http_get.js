'use strict'; // eslint-disable-line strict

const assert = require('assert');

const protocol = require('./protocol');
const httpUtils = require('./http_utils');
const split = require('./split');

function _isReplyBad(status) {
    return (status.error &&
            status.error.infos &&
            status.error.infos.status === 404);
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

            if (status.error &&
                (!worstError ||
                 worstError.infos.status < status.error.infos.status)) {
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
    const payload = {
        topic: httpUtils.topics.repair,
        key: opContext.fragments.objectKey,
        messages: [JSON.stringify(errorObj)],
    };

    errorAgent.send([payload], err => {
        if (err) {
            opContext.failedToPersist = true;
            opContext.log.error(
                'Failure to persist fragments to repair', err);
            // Return persistence error
            // TODO: not sure this a proper HTTP error....
            // TODO: must be a better way than disabling it
            /* eslint-disable no-param-reassign */
            err.infos = { status: 500, method: 'GET' };
            /* eslint-enable no-param-reassign */
            return callClientCb ? callback(err) : null;
        }

        /* Error persistence OK, return already encountered
         * error (might be null) */
        return callClientCb ? callback(currentError) : null;
    });
}

/**
 * Callback of framgnet GET query
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

    /* We are the first one successful, callback client */
    if (chunkStatus.nOk === 1 && status.response) {
        ret = callback(null, status.response);
    }

    /* Wait for all to answer */
    if (opContext.nPending !== 0) {
        return ret;
    }

    /* Nothing good so far
     *
     * Persist errors (like 404, corrupted or else)
     * Fail overall GET
     */
    const { error, errorHandler } = decideGET(opContext);
    if (errorHandler.fragments.length !== 0) {
        ret = _handleErrors(opContext, errorAgent, errorHandler,
                            callback, error, chunkStatus.nOk === 0);
    } else if (chunkStatus.nOk === 0) {
        ret = callback(error, null);
    }

    opContext.log.end();
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
      chunkId, fragmentId, requestTimeoutMs, range }) {
    const isData = fragmentId < opContext.fragments.nDataParts;
    const chunk = opContext.fragments.chunks[chunkId];
    const fragment = isData ? chunk.data[fragmentId] :
              chunk.coding[fragmentId - opContext.fragments.nDataParts];

    const reqContext = { opContext, chunkId, fragmentId };
    const { hostname, port, key } = fragment;
    const requestOptions = httpUtils.getCommonStoreRequestOptions(
        httpAgent, hostname, port, key);

    requestOptions.method = 'GET';
    requestOptions.headers = {
        ['Accept']: protocol.helpers.makeAccept(['data', range]),
    };

    const request = httpUtils.newRequest(
        requestOptions, opContext.log, reqContext,
        requestTimeoutMs,
        /* callback */
        reqCtx => _fragmentGETCb(reqCtx, callback, errorAgent));

    request.end();
}


/* eslint-disable no-unused-vars */
function _getChunkRange(fragments, chunkId, globalRange) {
    return globalRange;
}
/* eslint-enable no-unused-vars */

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
    // Split, replication or erasure coding is currently not supported
    assert.strictEqual(fragments.nChunks, 1);
    assert.strictEqual(fragments.nCodingParts, 0);

    const opContext = httpUtils.makeOperationContext(fragments, rawKey, log);

    /*
     * TODO: split? Request all? First 2 then the next when
     *       first is consumed? Introduced a small delay (1ms)?
     * TODO: handle range on split
     * TODO: erasure coding
     */
    split.getChunkSlice(fragments, range).forEach((chunk, chunkId) => {
        const chunkRange = _getChunkRange(fragments, chunkId, range);
        chunk.data.forEach((fragment, fragmentId) => {
            fragmentGET(
            { httpAgent, errorAgent, opContext, callback,
              chunkId, fragmentId, requestTimeoutMs, range: chunkRange });
        });
    });

    return opContext;
}

module.exports = {
    doGET,
};
