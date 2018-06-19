'use strict'; // eslint-disable-line strict

const http = require('http');
const assert = require('assert');
const werelogs = require('werelogs');

/* Temporary error, until everything gets done */
const notImplementedError = new Error('Not implemented yet');


class HyperdriveClient {
    /**
     * This represent our interface over several Hyperdrive servers
     * @constructor
     * @param {Object} [opts] - Contains the basic configuration.
     * @param {werelogs.API} [opts.logApi] - object providing a constructor
     *                                      function for the Logger object
     */
    constructor(opts) {
        const options = opts || {};
        this.httpAgent = new http.Agent({ keepAlive: true });
        this.setupLogging(options.logApi);
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

module.exports = HyperdriveClient;
