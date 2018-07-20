'use strict'; // eslint-disable-line strict

const assert = require('assert');

const protocol = require('./protocol');
const httpUtils = require('./http_utils');
const utils = require('./utils');

function _isReplyBad(replyInfos) {
    // Here error.infos stores HTTP return code as 'status'
    // Will be fixed later on with proper Arsenal errors
    // https://scality.atlassian.net/browse/RING-28704
    return (replyInfos.error &&
            replyInfos.error.infos &&
            replyInfos.error.infos.status !== 404);
}

/**
 * Decide what is the final status of the operation
 * and what to do on error fragments.
 *
 * @param {Object} opContext - Operation context
 * @return {Error|null} error - what to feed to client callback
 * @return {Object|null} errorHandler - Object tracking cleanup action
 */
function decideDELETE(opContext) {
    const toDelete = [];
    let worstError = null;
    let nValid = 0;

    opContext.status.forEach((chunk, chunkId) => {
        chunk.statuses.forEach((status, fragmentId) => {
            if (_isReplyBad(status)) {
                toDelete.push([chunkId, fragmentId]);
            } else {
                ++nValid;
            }

            if (utils.compareErrors(status.error, worstError) > 0) {
                worstError = status.error;
            }
        });
    });

    /* If we succeed to delete anything,
     * global status is success (as long as we manage
     * to persist toDelete with the errorAgent)
     */
    const error = nValid !== 0 ? null : worstError;
    if (toDelete.length === 0) {
        return { error, errorHandler: null };
    }

    const errorHandler = {
        fragments: toDelete,
        rawKey: opContext.rawKey,
    };
    return { error, errorHandler };
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
    const payload = {
        topic: httpUtils.topics.delete,
        key: opContext.fragments.objectKey,
        messages: [JSON.stringify(errorObj)],
    };

    errorAgent.send([payload], err => {
        if (err) {
            // Return persistence error
            // TODO: not sure this a proper HTTP error....
            // TODO: must be a better way than disabling it
            // eslint-disable-next-line no-param-reassign
            err.infos = { status: 500, method: 'DELETE' };
            opContext.log.error(
                'Failure to persist orphaned fragments', err);
            return callback(err);
        }

        /* Error persistence OK, return already encountered
         * error (might be null) */
        return callback(currentError);
    });
}

/**
 * Delete a single fragment
 *
 * @param {http.HttpAgent} httpAgent - Agent to use
 * @param {Object} errorAgent - Agent to persist errors
 * @param {Object} opContext - Operation context
 * @param {HyperdriveClient~deleteCallback} callback - Callback
 * @param {Number} chunkId - Current chunk number
 * @param {Number} fragmenId - Current fragment number
 * @param {Number} requestTimeoutMs - Timeout of each sub-query
 * @return {undefined}
 */
function fragmentDELETE(
    { httpAgent, errorAgent, opContext, callback,
      chunkId, fragmentId, requestTimeoutMs }) {
    const isData = fragmentId < opContext.fragments.nDataParts;
    const chunk = opContext.fragments.chunks[chunkId];
    const fragment = isData ? chunk.data[fragmentId] :
              chunk.coding[fragmentId - opContext.fragments.nDataParts];

    const reqContext = { opContext, chunkId, fragmentId };
    const { hostname, port, key } = fragment;
    const requestOptions = httpUtils.getCommonStoreRequestOptions(
        httpAgent, hostname, port, key);

    requestOptions.method = 'DELETE';
    requestOptions.headers = {
        ['Content-Length']: 0,
        ['Accept']: protocol.helpers.makeAccept(),
    };

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
            const { error, errorHandler } = decideDELETE(opCtx);
            if (errorHandler) {
                ret = _handleErrors(opContext, errorAgent,
                                    errorHandler,
                                    callback, error);
            } else {
                ret = callback(error);
            }
            opCtx.log.end().debug('End DELETE');

            return ret;
        });

    request.end();
}

/**
 * Delete all fragments of an object
 *
 * @param {http.HttpAgent} httpAgent - Agent to use
 * @param {Object} errorAgent - Agent to persist errors
 * @param {werelogs.Logger} logger - Logger to use
 * @param {Object} fragments - Object description to delete
 * @param {String} rawKey - Uri of the object
 *                 (refer to keyscheme.js for content)
 * @param {HyperdriveClient~deleteCallback} callback - Callback
 * @param {Number} requestTimeoutMs - Timeout of each sub-query
 * @return {Object} Operation context tracking everything
 */
function doDELETE({ httpAgent, errorAgent, log, fragments, rawKey,
                    callback, requestTimeoutMs }) {
    // Split is currently not supported
    assert.strictEqual(fragments.nChunks, 1);

    const opContext = httpUtils.makeOperationContext(fragments, rawKey, log);

    /* Send all at once */
    fragments.chunks.forEach((chunk, chunkId) => {
        [...chunk.data, ...chunk.coding].forEach((fragment, fragmentId) => {
            fragmentDELETE(
                { httpAgent, errorAgent, opContext, callback,
                  chunkId, fragmentId, requestTimeoutMs });
        });
    });

    return opContext;
}

module.exports = {
    doDELETE,
};
