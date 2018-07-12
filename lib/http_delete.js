'use strict'; // eslint-disable-line strict

const assert = require('assert');

const protocol = require('./protocol');
const httpUtils = require('./http_utils');

function _isReplyBad(status) {
    return (status.error &&
            status.error.infos &&
            status.error.infos.status !== 404);
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
    let nOk = 0;

    opContext.status.forEach((chunk, chunkId) => {
        chunk.statuses.forEach((status, fragmentId) => {
            if (_isReplyBad(status)) {
                toDelete.push([chunkId, fragmentId]);
            } else {
                ++nOk;
            }

            if (status.error &&
                (!worstError ||
                 worstError.infos.status < status.error.infos.status)) {
                worstError = status.error;
            }
        });
    });

    const error = nOk !== 0 ? null : worstError;
    const errorHandler = {
        toDelete,
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
            opContext.log.error(
                'Failure to persist orphaned fragments', err);
            // Return persistence error
            // TODO: not sure this a proper HTTP error....
            // TODO: must be a better way than disabling it
            /* eslint-disable no-param-reassign */
            err.infos = { status: 500 };
            /* eslint-disable no-param-reassign */
            return callback(err);
        }

        /* Error persistence OK, return already encountered
         * error (might be null) */
        return callback(currentError);
    });
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
                requestOptions, log, reqContext, requestTimeoutMs,
                /* callback */
                reqCtx => {
                    const opCtx = reqCtx.opContext;
                    opCtx.log.end();

                    /* Wait for all to answer */
                    if (opCtx.nPending !== 0) {
                        return null;
                    }

                    const { error, errorHandler } = decideDELETE(opCtx);
                    if (errorHandler.toDelete.length !== 0) {
                        _handleErrors(opContext, errorAgent,
                                      errorHandler,
                                      callback, error);
                        return null;
                    }

                    return callback(error);
                });

            request.end();
        });
    });

    return opContext;
}

module.exports = {
    doDELETE,
    decideDELETE,
};
