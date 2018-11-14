'use strict'; // eslint-disable-line strict

const fs = require('fs');
const http = require('http');
const kafka = require('node-rdkafka');
const werelogs = require('werelogs');

const config = require('./config');
const keyscheme = require('./keyscheme');
const httpGET = require('./http_get');
const httpPUT = require('./http_put');
const httpDELETE = require('./http_delete');
const utils = require('./utils');


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
     * @param {Object} [opts.errorAgentOptions] - Customize errorAgent
     *                                            (currently unspecified)
     * @comment For N replication, use data_parts=1 and
     * coding_parts=N (N+1 part in total)
     *
     * @throw config.InvalidConfigError for bad inputs
     */
    constructor(opts) {
        this.options = opts;

        const { config: conf, configIsValid, configError } =
                  config.validate(opts);
        if (!configIsValid) {
            throw configError;
        }
        this.conf = conf;

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
        this.setupErrorAgent(opts.errorAgent.kafkaBrokers);
        this.setupUuidMapping(opts.uuidmapping);
    }

    /**
     * Destroy connections kept alive by the client
     *
     * @return {undefined}
     */
    destroy() {
        this.httpAgent.destroy();
        this.destroyErrorAgent();
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
     * Setup agent persisting errors
     *
     * @param {String} kafkaBrokers - CSV list of hostnames for Producer to use
     * @return {undefined}
     */
    setupErrorAgent(kafkaBrokers) {
        /* Note that tests or scripts mocks an errorAgent
         * - scripts/server.js: logging to files
         * - tests/utils.js: keeping in-memory
         */
        const kafkaOptions = {
            'client.id': 'hdclient-rdkafka',
            'metadata.broker.list': kafkaBrokers,
            'socket.keepalive.enable': true,
            'socket.nagle.disable': true,
            'socket.timeout.ms': 30000,
        };

        this.errorAgent = new kafka.Producer(kafkaOptions);
    }

    /**
     * Load UUID -> host:port mapping
     * Use passed object or load JSON file
     *
     * @param {String|Object} uuidmapping - Mapping UUID on host:port
     * @return {undefined}
     */
    setupUuidMapping(uuidmapping) {
        if (typeof(uuidmapping) === 'string') {
            const loaded = fs.readFileSync(uuidmapping);
            this.uuidmapping = JSON.parse(loaded);
        } else {
            // Consider object as a valid map
            // This is used only for test and internal scripts anyway
            this.uuidmapping = uuidmapping;
        }
    }

    /**
     * Free error agent resources: memory, connections, etc
     * @return {undefined}
     */
    destroyErrorAgent() {
        this.errorAgent.close();
    }

    /** **************************************************************
     * Data backend interface
     *
     * Usage can be found at:
     * https://github.com/scality/S3/blob/development/8.0/lib/data/wrapper.js
     */

    /**
     * Find matching replication or erasure coding configuration
     * for this object.
     *
     * If multiple pattern match, the first one is returned.
     *
     * @param {String} bucket Name of the bucket
     * @param {String} object Object key
     * @return {Object} code if a matching conf is found
     * @return {String} code.type - Erasure coding 'RS' or replication 'CP'
     * @return {Number} code.dataParts - Number of data fragments
     * @return {Number} code.codingParts - Number of paity/coding fragments
     * @return {null} if no match found
     */
    selectCode(bucket, object) {
        const toMatch = `${bucket}/${object}`;
        for (let i = 0; i < this.conf.codes.length; i++) {
            const code = this.conf.codes[i];
            if (code.regex.test(toMatch)) {
                return code;
            }
        }

        return null;
    }

    /**
     * This sends a PUT request to hyperdrives.
     * @param {http.IncomingMessage} inputStream - Request with the data to send
     * @param {string} inputStream.contentHash - hash of the data to send
     * @param {integer} size - size
     * @param {Object} keyContext - parameters for key generation
     * @param {string} keyContext.bucketName - name of the object's bucket
     * @param {string} keyContext.objectKey - destination object key name
     * @param {String|Number} keyContext.versionId - S3 version of the object
     * @param {string} keyContext.owner - owner of the object
     * @param {string} keyContext.namespace - namespace of the S3 request
     * @param {string} reqUids - The serialized request id
     * @param {HyperdriveClient~putCallback} callback - callback
     * @param {boolean} force - Specify hyperdive must accept PUT request,
     *                          even if congested
     * @returns {Object} Operation context - can be used to wait
     *                                       for all pending ops
     */
    put(inputStream, size, keyContext, reqUids, callback, force = true) {
        const log = this.createLogger(reqUids);

        const code = this.selectCode(
            keyContext.bucketName, keyContext.objectKey);
        if (code === undefined || code === null) {
            const enhancedError = utils.mockedArsenalError(
                'ConfigError', 422,
                'No matching code pattern found');
            callback(enhancedError);
            return null;
        }

        // Select hyperdrives
        const fragments = keyscheme.keygen(
            this.conf.serviceId,
            this.conf.policy,
            keyContext,
            size,
            code.type,
            code.dataParts,
            code.codingParts
        );
        if (!fragments) {
            const error = utils.mockedArsenalError(
                'PlacementError', 500,
                'Failed to select enough hyperdrives to store data');
            callback(error, null);
        }

        const rawKey = keyscheme.serialize(fragments);

        return httpPUT.doPUT(
            { log, fragments, rawKey,
              callback, size, inputStream,
              httpAgent: this.httpAgent,
              errorAgent: this.errorAgent,
              requestTimeoutMs: this.conf.requestTimeoutMs,
              uuidmapping: this.uuidmapping,
              immutable: this.conf.immutable,
              force,
            });
    }

    /**
     * This sends a GET request to hyperdrives.
     * @param {String} rawKey - The key associated to the value
     * @param {Number[] | Undefined} range - Range (if any) with
     *                                       first element the start
     *                                       and the second element the end
     * @param {String} reqUids - The serialized request id
     * @param {HyperdriveClient~getCallback} callback - callback
     * @returns {Object} Operation context - can be used to wait
     *                                       for all pending ops
     */
    get(rawKey, range, reqUids, callback) {
        let fragments;
        try {
            fragments = keyscheme.deserialize(rawKey);
        } catch (error) {
            const enhancedError = utils.mockedArsenalError(
                'ParseError', 400,
                `Failed to parse input key: ${error.message}`);
            callback(enhancedError);
            return null;
        }

        const log = this.createLogger(reqUids);
        return httpGET.doGET(
            { fragments, rawKey, range, callback, log,
              httpAgent: this.httpAgent,
              errorAgent: this.errorAgent,
              requestTimeoutMs: this.conf.requestTimeoutMs,
              uuidmapping: this.uuidmapping,
            });
    }

    /**
     * This sends a DELETE request to hyperdrives.
     * @param {String} rawKey - Uri of the object
     *                 (refer to keyscheme.js for content)
     * @param {String} reqUids - The serialized request id
     * @param {HyperdriveClient~deleteCallback} callback - callback
     * @returns {Object} Operation context - can be used to wait
     *                                       for all pending ops
     */
    delete(rawKey, reqUids, callback) {
        let fragments;
        try {
            fragments = keyscheme.deserialize(rawKey);
        } catch (error) {
            const enhancedError = utils.mockedArsenalError(
                'ParseError', 400,
                `Failed to parse input key: ${error.message}`);
            callback(enhancedError);
            return null;
        }

        const log = this.createLogger(reqUids);
        return httpDELETE.doDELETE(
            { fragments, rawKey, callback, log,
              httpAgent: this.httpAgent,
              errorAgent: this.errorAgent,
              requestTimeoutMs: this.conf.requestTimeoutMs,
              uuidmapping: this.uuidmapping,
            });
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
        /* Until we have a proper healthcheck or deem it unnecessary
         *
         * XXX - Ugly hack alert
         * The caller
         * (https://github.com/scality/S3/
         *    blob/development/8.0/lib/data/wrapper.js)
         * does not use what is returned as a stream per se.
         * Only uses statusCode and statusMessage fields, so
         * we duck type it here.
         */
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
 * @param {Object} - statusCode and statusMessage to forward
 */

module.exports = {
    HyperdriveClient,
};
