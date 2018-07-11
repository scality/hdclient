'use strict'; // eslint-disable-line strict

const http = require('http');
const werelogs = require('werelogs');

const config = require('./config');
const keyscheme = require('./keyscheme');
const doGET = require('./http_get');
const doPUT = require('./http_put');
const doDELETE = require('./http_delete');


class HyperdriveClient {
    /**
     * This represent our interface over several Hyperdrive servers
     * @constructor
     * @param {Object} [opts] - Contains the basic configuration.
     * @param {werelogs.API} [opts.logApi] - Object providing a constructor
     *                                      function for the Logger object
     * @param {Object} [opts.policy] - Data placement policy
     * @param {Number} [opts.dataParts] - Number of data parts
     * @param {Number} [opts.codingParts] - Number of coding parts
     * @param {Number} [opts.requestTimeoutMs] - Timeout on answer in ms
     * @comment For N replication, use data_parts=1 and
     * coding_parts=N (N+1 part in total)
     *
     * @throw config.InvalidConfigError for bad inputs
     */
    constructor(opts) {
        this.options = opts;

        const { configIsValid, configError } = config.validate(opts);
        if (!configIsValid) {
            throw configError;
        }

        /* Set backend type as 'scality'
         * Ths will indicate S3/CloudServer to treat it
         * specifically (e.g. as SproxydClient)
         *
         * DO NOT CHANGE OR 'GET' IS BROKEN
         */
        this.clientType = 'scality';

        this.httpAgent = new http.Agent({
            keepAlive: true,
            keepAliveMsecs: 30000, // 30s
            /* Max. number of opened connections towards same hyperdrive */
            maxSockets: 500,
        });
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
        const log = this.createLogger(reqUids);

        // Select hyperdrives
        const fragments = keyscheme.keygen(
            this.options.policy,
            keyContext.objectKey,
            size,
            'CP', // replication only for now
            this.options.dataParts,
            this.options.codingParts
        );

        const rawKey = keyscheme.serialize(fragments);

        doPUT({ log, fragments, rawKey,
                callback, size, stream,
                httpAgent: this.httpAgent,
                requestTimeoutMs: this.options.requestTimeoutMs });
    }

    /**
     * This sends a GET request to hyperdrives.
     * @param {String} rawKey - The key associated to the value
     * @param {Number [] | Undefined} range - Range (if any) with
     *                                        first element the start
     *                                        and the second element the end
     * @param {String} reqUids - The serialized request id
     * @param {HyperdriveClient~getCallback} callback - callback
     * @returns {undefined}
     */
    get(rawKey, range, reqUids, callback) {
        const log = this.createLogger(reqUids);
        let fragments;
        try {
            fragments = keyscheme.deserialize(rawKey);
        } catch (error) {
            log.error(`Failed to deserialize key: ${rawKey}`,
                      error.message);
            callback(error);
            log.end();
            return;
        }

        doGET({ fragments, rawKey, range, callback, log,
                httpAgent: this.httpAgent,
                requestTimeoutMs: this.options.requestTimeoutMs });
    }

    /**
     * This sends a DELETE request to hyperdrives.
     * @param {String} rawKey - Uri of the object
     *                 (refer to keyscheme.js for content)
     * @param {String} reqUids - The serialized request id
     * @param {HyperdriveClient~deleteCallback} callback - callback
     * @returns {undefined}
     */
    delete(rawKey, reqUids, callback) {
        const log = this.createLogger(reqUids);
        let fragments;
        try {
            fragments = keyscheme.deserialize(rawKey);
        } catch (error) {
            log.error(`Failed to deserialize key: ${rawKey}`,
                      error.message);
            callback(error);
            log.end();
            return;
        }

        doDELETE({ fragments, rawKey, callback, log,
                   httpAgent: this.httpAgent,
                   requestTimeoutMs: this.options.requestTimeoutMs });
    }

    /**
     * Verify hyperdrive statuses
     *
     * @param {Object} log - The log from s3
     * @param {HyperdriveClient-healthcheckCallback} callback - callback
     * @returns {undefined}
     * @comment Mandatory for Scality clientType
     */
    healthcheck(log, callback) {
        /* Until we have a proper healthcheck or deem it unnecessary */
        process.nextTick(() => {
            callback(null, {
                statusCode: 200,
                statusMessage: 'Alive and kicking',
            });
        });
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

/**
 * @callback HyperdriveClient-healthcheckCallback
 * @param {Error} - The encountered error
 * @param {stream.Readable} stream - The stream of values fetched
 */

module.exports = {
    HyperdriveClient,
};
