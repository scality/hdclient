'use strict'; // eslint-disable-line strict

const http = require('http');
const assert = require('assert');
const werelogs = require('werelogs');

const config = require('./config');
const protocol = require('./protocol');
const keyscheme = require('./keyscheme');
const httpUtils = require('./http_utils');


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
        // Select hyperdrives
        const fragments = keyscheme.keygen(
            this.options.policy,
            keyContext.objectKey,
            size,
            'CP', // replication only for now
            this.options.dataParts,
            this.options.codingParts
        );

        const rawGenKey = keyscheme.serialize(fragments);

        // Split, replication or erasure coding is currently not supported
        assert.strictEqual(fragments.nChunks, 1);
        assert.strictEqual(fragments.nDataParts, 1);
        assert.strictEqual(fragments.nCodingParts, 0);
        const { hostname, port, key } = fragments.chunks[0].data[0];

        const log = this.createLogger(reqUids);
        const contentType = protocol.helpers.makePutContentType(
            { data: size } /* Only 'data' payload is supported for now */
        );
        const requestOptions = this._getCommonStoreRequestOptions(
            hostname, port, key
        );
        requestOptions.method = 'PUT';
        requestOptions.headers = {
            ['Content-Length']: size,
            ['Content-Type']: contentType,
        };

        const opContext = httpUtils.makeOperationContext(fragments);
        const reqContext = {
            opContext,
            chunkId: 0,
            fragmentId: 0,
        };

        const request = httpUtils.newRequest(
            requestOptions, log, reqContext,
            this.options.requestTimeoutMs,
            /* callback */
            opCtx => {
                log.end();
                const endStatus = opCtx.status[0].statuses[0];
                if (endStatus.timeout) {
                    return callback(null, rawGenKey);
                }

                return callback(endStatus.error, rawGenKey);
            });

        // TODO abstract a bit stream to have a safer interface
        stream.pipe(request);
        stream.on('error', err => {
            // forward error downstream
            request.emit('error', err);
        });
    }

    /**
     * This sends a GET request to hyperdrives.
     * @param {String} rawKey - The key associated to the value
     * @param {Number [] | Undefined} range - range (if any) with
     *                                         first element the start
     * and the second element the end
     * @param {String} reqUids - The serialized request id
     * @param {HyperdriveClient~getCallback} callback - callback
     * @returns {undefined}
     */
    get(rawKey, range, reqUids, callback) {
        let fragments;
        try {
            fragments = keyscheme.deserialize(rawKey);
        } catch (error) {
            callback(error);
            return;
        }

        // Split, replication or erasure coding is currently not supported
        assert.strictEqual(fragments.nChunks, 1);
        assert.strictEqual(fragments.nDataParts, 1);
        assert.strictEqual(fragments.nCodingParts, 0);
        const { hostname, port, key } = fragments.chunks[0].data[0];

        const log = this.createLogger(reqUids);
        const requestOptions = this._getCommonStoreRequestOptions(
            hostname, port, key
        );
        requestOptions.method = 'GET';
        requestOptions.headers = {
            ['Accept']: protocol.helpers.makeAccept(['data', range]),
        };

        const opContext = httpUtils.makeOperationContext(fragments);
        const reqContext = {
            opContext,
            chunkId: 0,
            fragmentId: 0,
        };

        const request = httpUtils.newRequest(
            requestOptions, log, reqContext,
            this.options.requestTimeoutMs,
            /* callback */
            opCtx => {
                log.end();
                return callback(opCtx.status[0].statuses[0].error,
                                opCtx.status[0].statuses[0].response);
            });

        request.end();
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
        let fragments;
        try {
            fragments = keyscheme.deserialize(rawKey);
        } catch (error) {
            callback(error);
            return;
        }

        // Split, replication or erasure coding is currently not supported
        assert.strictEqual(fragments.nChunks, 1);
        assert.strictEqual(fragments.nDataParts, 1);
        assert.strictEqual(fragments.nCodingParts, 0);
        const { hostname, port, key } = fragments.chunks[0].data[0];

        const log = this.createLogger(reqUids);
        const requestOptions = this._getCommonStoreRequestOptions(
            hostname, port, key
        );
        requestOptions.method = 'DELETE';
        requestOptions.headers = {
            ['Content-Length']: 0,
            ['Accept']: protocol.helpers.makeAccept(),
        };

        const opContext = httpUtils.makeOperationContext(fragments);
        const reqContext = {
            opContext,
            chunkId: 0,
            fragmentId: 0,
        };

        const request = httpUtils.newRequest(
            requestOptions, log, reqContext,
            this.options.requestTimeoutMs,
            /* callback */
            opCtx => {
                log.end();
                return callback(opCtx.status[0].statuses[0].error);
            });

        request.end();
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
