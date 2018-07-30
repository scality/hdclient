'use strict'; // eslint-disable-line strict

const http = require('http');

const protocol = require('./protocol');
const utils = require('./utils');

/**
 * Kafka topics name
 */
const topics = {
    check: 'check',
    delete: 'delete',
    repair: 'repair',
};

/**
 * Create a new Operation Context
 *
 * @param {Object} fragments Deserialized or generated object fragments
 * @param {String} rawKey - Uri of the object
 *                 (refer to keyscheme.js for content)
 * @param {werelogs.Logger} log - Logger of the query
 * @return {Object} Operation context
 * Fields of interests:
 * - fragments: reference to passed argument
 * - status {[Object]}: length = fragments.nChunks
 *     - nOk, nError, nTimeout {Number}: counter for fragments status
 *     - statuses {[Object]}: details of each fragment status
 *         - response {Null|httpResponse}
 *         - error {Null|Error}
 *         - timeout {boolean}
 */
function makeOperationContext(fragments, rawKey, log) {
    const nParts = fragments.nDataParts + fragments.nCodingParts;
    const opContext = {
        log,
        rawKey,
        fragments,
        status: utils.range(fragments.nChunks).map(
            () => ({
                nOk: 0,
                nError: 0,
                nTimeout: 0,
                statuses: Array(nParts),
            })),
        nPending: fragments.nChunks * nParts,
    };

    return opContext;
}

function _updateOperationContext(reqContext,
                                 { response = null,
                                   error = null,
                                   timeout = false }) {
    const opContext = reqContext.opContext;
    const chunk = opContext.status[reqContext.chunkId];
    if (chunk.statuses[reqContext.fragmentId]) {
        return false;
    }

    if (timeout === true) {
        ++chunk.nTimeout;
    } else if (error !== null) {
        ++chunk.nError;
    } else {
        ++chunk.nOk;
    }

    --opContext.nPending;
    chunk.statuses[reqContext.fragmentId] = {
        response,
        error,
        timeout,
    };

    return true;
}

/**
 * Handle success of fragment HTTP request
 *
 * @param {Object} reqContext to use
 * @param {function} callback opContext -> ?
 * @param {http.IncomingMessage} response of query
 * @return {null|Any} Anything callback returned, null if not called
 */
function _handleHttpSuccess(reqContext, callback, response) {
    let ret = null;
    if (_updateOperationContext(reqContext, { response })) {
        ret = callback(reqContext);
    }

    /* Very important!
     * Stream is paused, we must consume it, otherwise
     * socket is left as alive and is not reused.
     *
     * The callback should either:
     * - be listening on 'data' to read the body
     * - doing nothing to drain the socket
     *
     * TODO add specific test with very low maxSocket to verify if
     *      they are correctly free
     */
    response.resume();
    return ret;
}

/**
 * Handle error of fragment HTTP request
 *
 * @param {Object} reqContext to use
 * @param {function} callback opContext -> ?
 * @param {Object} errInfos Extra information to attach on error
 * @returns {Any} Anything callback returned or null if not called
 */
function _handleHttpError(reqContext, callback, errInfos) {
    const error = new Error('HTTP request failed');
    error.infos = errInfos;
    if (_updateOperationContext(reqContext, { error })) {
        return callback(reqContext);
    }

    return null;
}

/**
 * Create new HTTP request
 *
 * @param {Object} options to pass to the request
 * @param {werelogs.Logger} logger to use
 * @param {Object} reqContext Request context
 * @param {function} callback to call on reply
 * @return {http.ClientRequest} created request
 */
function _createHttpRequest(options, logger, reqContext, callback) {
    const request = http.request(options, response => {
        if (request.aborted) {
            /* Request was already handled/stopped
             * Abort caller should handle the error
             * (calling callback and all) so we do nothing here.
             */
            return null;
        }

        clearTimeout(request.connection.timeoutTimerId);

        const success = Math.floor(response.statusCode / 200);
        if (success !== 1) {
            const errInfos = {
                status: response.statusCode,
                headers: response.headers,
                method: options.method,
            };
            logger.error(`${response.method} ${response.url}:`, errInfos);
            return _handleHttpError(reqContext, callback, errInfos);
        }

        return _handleHttpSuccess(reqContext, callback, response);
    });

    return request;
}

/**
 * Send a HTTP request
 *
 * @param {Object} options Object filled with all necessary
 *                 options and headers. See getCommonStoreRequestOptions
 * @param {werelogs.Logger} logger to use
 * @param {Object} reqContext Request context
 * @param {Number} timeoutMs Request timeout
 * @param {function} callback (opContext) -> ?
 * @return {null|Object} whatever vallback returns, null if not called
 */
function newRequest(options, logger, reqContext, timeoutMs, callback) {
    const enhanceLogs = {
        path: options.path,
        port: options.port,
        method: options.method,
        host: options.hostname,
    };

    const request = _createHttpRequest(
        options, logger, reqContext, callback);

    // disable nagle algorithm
    request.setNoDelay(true);

    request.on('error', error => {
        let ret = null;
        logger.error(error.message, enhanceLogs);
        // TODO: avoid this hack?
        /* eslint-disable no-param-reassign */
        error.infos = enhanceLogs;
        error.infos.status = 500; // HTTP return code
        error.infos.method = options.method;
        /* eslint-disable no-param-reassign */
        if (_updateOperationContext(reqContext, { error })) {
            ret = callback(reqContext);
        }
        request.destroy();
        return ret;
    });

    // Socket inactivity timeout
    request.timeoutTimerId = request.socket.setTimeout(
        timeoutMs,
        () => {
            let ret = null;
            logger.error('Timeout', enhanceLogs);
            const error = new Error('Timeout');
            error.infos = enhanceLogs;
            error.infos.status = 500; // HTTP return code
            error.infos.method = options.method;
            if (_updateOperationContext(reqContext, { error, timeout: true })) {
                ret = callback(reqContext);
                request.abort();
            }
            return ret;
        });

    return request;
}

/**
 * Construct option object to use for next store query
 *
 * @param  {http.HttpAgent} httpAgent to use
 * @param {String} hostname to contact
 * @param {Number} port to contact on
 * @param {String} key to target
 * @return {Object} HTTP client request object filled with common options
 */
function getCommonStoreRequestOptions(httpAgent, hostname, port, key) {
    return {
        hostname,
        port,
        path: `${protocol.specs.STORAGE_BASE_URL}/${key}`,
        agent: httpAgent,
        protocol: 'http:',
        encoding: null, // Binary
    };
}


module.exports = {
    getCommonStoreRequestOptions,
    newRequest,
    makeOperationContext,
    topics,
};
