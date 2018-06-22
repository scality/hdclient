'use strict'; // eslint-disable-line strict

const http = require('http');
const assert = require('assert');
const werelogs = require('werelogs');

const config = require('./config');
const protocol = require('./protocol');

/* Temporary error, until everything gets done */
const notImplementedError = new Error('Not implemented yet');

class HyperdriveClient {
    /**
     * This represent our interface over several Hyperdrive servers
     * @constructor
     * @param {Object} [opts] - Contains the basic configuration.
     * @param {werelogs.API} [opts.logApi] - Object providing a constructor
     *                                      function for the Logger object
     * @param {String} [opts.endpoints] - List of DNS resolvable hyperdrive
     *                                    endpoints
     * @param {Number} [opts.dataParts] - Number of data parts
     * @param {Number} [opts.codingParts] - Number of coding parts
     * @comment For N replication, use data_parts=1 and
     * coding_parts=N (N+1 part in total)
     *
     * @throw config.InvalidConfigError for bad inputs
     */
    constructor(opts) {
        this.options = opts;
        const [configIsValid, configError] = config.validate(opts);
        if (!configIsValid) {
            throw configError;
        }

        this.httpAgent = new http.Agent({ keepAlive: true });
        this.setupLogging(opts.logApi);
    }

    /**
     * Destroy connections kept alive by the client
     *
     * @return {undefined}
     */
    destroy() {
        this.httpAgent.destroy();
    }

    /**
     * Create a dedicated logger for HyperdriveClient, from the provided
     *  werelogs API instance.
     *
     * @param {werelogs.API} [logApi] - object providing a constructor function
     *                                for the Logger object
     * @return {undefined}
     */
    setupLogging(logApi) {
        this.logging = new (logApi || werelogs).Logger('HyperdriveClient');
    }

    createLogger(reqUids) {
        return reqUids ?
            this.logging.newRequestLoggerFromSerializedUids(reqUids) :
            this.logging.newRequestLogger();
    }

    /**
     * Construct option object to use for next store query
     *
     * @param {String} hostname to contact
     * @param {Number} port to contact on
     * @param {String} key to target
     * @returns {Object} HTTP client request object filled with common options
     */
    _getCommonStoreRequestOptions(hostname, port, key) {
        return {
            hostname,
            port,
            path: `${protocol.specs.STORAGE_BASE_URL}/${key}`,
            agent: this.httpAgent,
            protocol: 'http:',
        };
    }

    /**
     * Send a HTTP request
     *
     * @param {Object} options Object filled with all necessary
     *                 options and headers. See _getCommonStoreRequestOptions
     * @param {werelogs.Logger} logger to use
     * @param {function} callback (HTTPError/null, http.IncomingMessage) -> ?
     * @returns {http.ClientRequest} object
     */
    _newRequest(options, logger, callback) {
        const request = http.request(options, response => {
            if (response.statusCode !== 200) {
                const errInfos = {
                    status: response.statusCode,
                    headers: response.headers,
                    method: response.method,
                };
                logger.error(`${response.method} ${response.url}:`, errInfos);
                const error = new Error('HTTP request failed');
                error.infos = errInfos;
                callback(error);
                return;
            }
            callback(null /* error */, response);
        }).on('error', callback);
        // disable nagle algorithm
        request.setNoDelay(true);

        return request;
    }

    /** **************************************************************
     * Data backend interface
     *
     * Usage can be found at:
     * https://github.com/scality/S3/blob/development/8.0/lib/data/wrapper.js
     */

    /**
     * This sends a PUT request to hyperdrives.
     * @param {http.IncomingMessage} stream - Request with the data to send
     * @param {string} stream.contentHash - hash of the data to send
     * @param {integer} size - size
     * @param {Object} keyContext - parameters for key generation
     * @param {string} keyContext.bucketName - name of the object's bucket
     * @param {string} keyContext.objectKey: destination object key name
     * @param {string} keyContext.owner - owner of the object
     * @param {string} keyContext.namespace - namespace of the S3 request
     * @param {string} reqUids - The serialized request id
     * @param {HyperdriveClient~putCallback} callback - callback
     * @returns {undefined}
     */
    put(stream, size, keyContext, reqUids, callback) {
        // const log = this.createLogger(reqUids);
        callback(notImplementedError);
    }

    /**
     * This sends a GET request to hyperdrives.
     * @param {String} key - The key associated to the value
     * @param {Number [] | Undefined} range - range (if any) with
     *                                         first element the start
     * and the second element the end
     * @param {String} reqUids - The serialized request id
     * @param {HyperdriveClient~getCallback} callback - callback
     * @returns {undefined}
     */
    get(key, range, reqUids, callback) {
        assert.strictEqual(typeof key, 'string');
        // const log = this.createLogger(reqUids);
        callback(notImplementedError);
    }

    /**
     * This sends a HEAD request to hyperdrives.
     * @param {String} key - The key to get from datastore
     * @param {String} reqUids - The serialized request id
     * @param {HyperdriveClient~getCallback} callback - callback
     * @returns {undefined}
     */
    head(key, reqUids, callback) {
        assert.strictEqual(typeof key, 'string');
        // const log = this.createLogger(reqUids);
        callback(notImplementedError);
    }


    /**
     * This sends a DELETE request to hyperdrives.
     * @param {String} key - The key associated to the values
     * @param {String} reqUids - The serialized request id
     * @param {HyperdriveClient~deleteCallback} callback - callback
     * @returns {undefined}
     */
    delete(key, reqUids, callback) {
        assert.strictEqual(typeof key, 'string');
        // const log = this.createLogger(reqUids);
        callback(notImplementedError);
    }
}

/**
 * @callback HyperdriveClient~putCallback
 * @param {Error} - The encountered error
 * @param {String} key - The key to access the data
 */

/**
 * @callback HyperdriveClient~getCallback
 * @param {Error} - The encountered error
 * @param {stream.Readable} stream - The stream of values fetched
 */

/**
 * @callback HyperdriveClient~deleteCallback
 * @param {Error} - The encountered error
 */

module.exports = {
    HyperdriveClient,
};
