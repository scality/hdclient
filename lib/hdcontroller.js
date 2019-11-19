'use strict'; // eslint-disable-line strict
Object.defineProperty(exports, "__esModule", { value: true });
const assert = require("assert");
const async = require("async");
const http = require("http");
const werelogs = require("werelogs");
const shuffle_1 = require("./shuffle");
class HDProxydError extends Error {
    constructor() {
        super(...arguments);
        this.isExpected = false;
    }
}
exports.HDProxydError = HDProxydError;
/*
 * This handles the request, and the corresponding response default behaviour
 */
function _createRequest(req, log, callback) {
    const request = http.request(req, (response) => {
        // Get range returns a 206
        // Concurrent deletes on hdproxyd/immutable keys returns 423
        if (response.statusCode !== 200 && response.statusCode !== 206 &&
            !(response.statusCode === 423 && req.method === 'DELETE')) {
            const error = new HDProxydError();
            error.code = response.statusCode;
            error.isExpected = true;
            log.debug('got expected response code:', { statusCode: response.statusCode });
            return callback(error);
        }
        return callback(undefined, response);
    }).on('error', callback);
    // disable nagle algorithm
    request.setNoDelay(true);
    return request;
}
/*
 * This parses an array of strings representing our bootstrap list of
 * the following form: [ 'hostname:port', ... , 'hostname.port' ]
 * into an array of [hostname, port] arrays.
 * Since the bootstrap format may change in the future, having this
 * contained in a separate function will make things easier to
 * maintain.
 */
function _parseBootstrapList(list) {
    return list.map((value) => value.split(':'));
}
class HDProxydClient {
    /**
     * This represent our interface with the hdproxyd server.
     * @constructor
     * @param {Object} [opts] - Contains the basic configuration.
     * @param {string[]} [opts.bootstrap] - list of hdproxyd servers,
     *      of the form 'hostname:port'
     * @param {string} [opts.path] - default to /store/
     * @param {werelogs.API} [opts.logApi] - object providing a constructor
     *                                      function for the Logger object
     */
    constructor(opts) {
        this.current = ['', ''];
        const options = opts || {};
        this.bootstrap = opts.bootstrap === undefined ?
            [['localhost', '18888']] : _parseBootstrapList(opts.bootstrap);
        this.bootstrap = shuffle_1.shuffle(this.bootstrap);
        this.path = '/store/';
        this.setCurrentBootstrap(this.bootstrap[0]);
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
    /*
     * Create a dedicated logger for HDProxyd, from the provided werelogs API
     * instance.
     *
     * @param {werelogs.API} [logApi] - object providing a constructor function
     *                                for the Logger object
     * @return {undefined}
     */
    setupLogging(logApi) {
        this.logging = new (logApi || werelogs).Logger('HDProxydClient');
    }
    createLogger(reqUids) {
        return reqUids ?
            this.logging.newRequestLoggerFromSerializedUids(reqUids) :
            this.logging.newRequestLogger();
    }
    _shiftCurrentBootstrapToEnd(log) {
        const previousEntry = this.bootstrap.shift();
        this.bootstrap.push(previousEntry);
        const newEntry = this.bootstrap[0];
        this.setCurrentBootstrap(newEntry);
        log.debug(`bootstrap head moved from ${previousEntry} to ${newEntry}`);
        return this;
    }
    setCurrentBootstrap(host) {
        this.current = host;
        return this;
    }
    getCurrentBootstrap() {
        return this.current;
    }
    /**
     * Returns the first id from the array of request ids.
     * @param {Object} log - log from s3
     * @returns {String} - first request id
     */
    _getFirstReqUid(log) {
        let reqUids = [];
        if (log) {
            reqUids = log.getUids();
        }
        return reqUids[0];
    }
    /*
     * This creates a default request for hdproxyd
     */
    _createRequestHeader(method, headers, key, params, log) {
        const reqHeaders = headers || {};
        const currentBootstrap = this.getCurrentBootstrap();
        const reqUids = this._getFirstReqUid(log);
        reqHeaders['content-type'] = 'application/octet-stream';
        reqHeaders['X-Scal-Request-Uids'] = reqUids;
        reqHeaders['X-Scal-Trace-Ids'] = reqUids;
        if (params && params.range) {
            /* eslint-disable dot-notation */
            reqHeaders.Range = `bytes=${params.range[0]}-${params.range[1]}`;
            /* eslint-enable dot-notation */
        }
        let realPath;
        if (key === '/job/delete') {
            realPath = key;
        }
        else {
            realPath = key ? `${this.path}${key}` : this.path;
        }
        return {
            hostname: currentBootstrap[0],
            port: currentBootstrap[1],
            method,
            path: realPath,
            headers: reqHeaders,
            agent: this.httpAgent,
        };
    }
    _failover(method, stream, size, key, tries, log, callback, params, payload) {
        const args = params === undefined ? {} : params;
        let counter = tries;
        log.debug('sending request to hdproxyd', { method, key, args, counter });
        let receivedResponse = false;
        this._handleRequest(method, stream, size, key, log, (err, ret) => {
            if ((err && !err.isExpected) || !ret) {
                if (receivedResponse === true) {
                    log.fatal('multiple responses from hdproxyd, trying to ' +
                        'write more data to the stream after hdproxyd sent a ' +
                        'response, size of the object could be incorrect', {
                        error: err,
                        method: '_failover',
                        size,
                        objectKey: key,
                    });
                    return undefined;
                }
                if (++counter >= this.bootstrap.length) {
                    log.errorEnd('failover tried too many times, giving up', { retries: counter });
                    return callback(err);
                }
                return this._shiftCurrentBootstrapToEnd(log)
                    ._failover(method, stream, size, key, counter, log, callback, params);
            }
            receivedResponse = true;
            log.end().debug('request received response');
            return callback(err, ret);
        }, args, payload);
    }
    /*
     * This does a basic routing of the methods, dealing with the request
     * creation and its sending.
     */
    _handleRequest(method, stream, size, key, log, callback, params, payload) {
        const headers = params.headers ? params.headers : {};
        const req = this._createRequestHeader(method, headers, key, params, log);
        const host = this.getCurrentBootstrap();
        const isBatchDelete = key === '/job/delete';
        if (stream) {
            headers['content-length'] = size;
            const request = _createRequest(req, log, (err, response) => {
                if (err || !response) {
                    log.error('putting chunk to hdproxyd', { host, key,
                        error: err });
                    return callback(err);
                }
                const Method = method;
                log.debug('createRequest cb', { method: Method,
                    hdr: response.headers });
                const realKey = response.headers['scal-key'] ?
                    response.headers['scal-key'] : 'unk';
                // We return the key
                log.debug('stored to hdproxyd', { host, key: realKey,
                    statusCode: response.statusCode });
                return callback(undefined, response);
            });
            request.on('finish', () => log.debug('finished sending PUT chunks to hdproxyd', {
                component: 'sproxydclient',
                method: '_handleRequest',
                contentLength: size,
            }));
            stream.pipe(request);
            stream.on('error', (err) => {
                log.error('error from readable stream', {
                    error: err,
                    method: '_handleRequest',
                    component: 'sproxydclient',
                });
                request.end();
            });
        }
        else {
            headers['content-length'] = isBatchDelete ? size : 0;
            const request = _createRequest(req, log, (err, response) => {
                if (err || !response) {
                    log.error('error sending hdproxyd request', { host,
                        error: err, key, method: '_handleRequest' });
                    return callback(err);
                }
                log.debug('success sending hdproxyd request', { host,
                    statusCode: response.statusCode, key,
                    method: '_handleRequest' });
                return callback(undefined, response);
            });
            request.end(payload);
        }
    }
    /**
     * This sends a PUT request to hdproxyd.
     * @param {Stream} stream - Request with the data to send
     * @param {string} stream.contentHash - hash of the data to send
     * @param {integer} size - size
     * @param {Object} params - parameters for key generation
     * @param {string} params.bucketName - name of the object's bucket
     * @param {string} params.owner - owner of the object
     * @param {string} params.namespace - namespace of the S3 request
     * @param {string} reqUids - The serialized request id
     * @param {HDProxydClient~putCallback} callback - callback
     * @returns {undefined}
     */
    put(stream, size, params, reqUids, callback) {
        const log = this.createLogger(reqUids);
        this._failover('POST', stream, size, '', 0, log, (err, response) => {
            if (err || !response) {
                return callback(err);
            }
            if (!response.headers['scal-key']) {
                return callback(new HDProxydError('no key returned'));
            }
            const key = response.headers['scal-key'];
            response.resume(); // drain the stream
            response.on('end', () => {
                return callback(undefined, key);
            });
        }, params);
    }
    /**
     * This sends a GET request to hdproxyd.
     * @param {String} key - The key associated to the value
     * @param { Number [] | Undefined} range - range (if any) with
     *                                         first element the start
     * and the second element the end
     * @param {String} reqUids - The serialized request id
     * @param {HDProxydClient~getCallback} callback - callback
     * @returns {undefined}
     */
    get(key, range, reqUids, callback) {
        assert.strictEqual(typeof key, 'string');
        const log = this.createLogger(reqUids);
        const params = { range };
        this._failover('GET', null, 0, key, 0, log, callback, params);
    }
    /**
     * This sends a DELETE request to hdproxyd.
     * @param {String} key - The key associated to the values
     * @param {String} reqUids - The serialized request id
     * @param {HDProxydClient~deleteCallback} callback - callback
     * @returns {undefined}
     */
    delete(key, reqUids, callback) {
        assert.strictEqual(typeof key, 'string');
        const log = this.createLogger(reqUids);
        this._failover('DELETE', null, 0, key, 0, log, (err, res) => {
            if (res) {
                // Drain the stream
                res.resume();
                res.on('end', () => {
                    callback(err);
                });
            }
            else {
                callback(err);
            }
        });
    }
    /**
     * This sends a BATCH DELETE request to hdproxyd.
     * @param {Object} list - object containing a list of keys to delete
     * @param {Array} list.keys - array of string keys to delete
     * @param {String} reqUids - The serialized request id
     * @param {SproxydClient~deleteCallback} callback - callback
     * @returns {void}
     */
    batchDelete(list, reqUids, callback) {
        assert.strictEqual(typeof list, 'object');
        assert(list.keys.every((k) => typeof k === 'string'));
        // split the list into batches of 1000 each
        const batches = [];
        while (list.keys.length > 0) {
            batches.push({ keys: list.keys.splice(0, 1000) });
        }
        async.eachLimit(batches, 5, (b, done) => {
            const log = this.createLogger(reqUids);
            const payload = Buffer.from(JSON.stringify(b.keys));
            this._failover('POST', null, payload.length, '/job/delete', 0, log, (err, res) => {
                if (res) {
                    // Drain the stream
                    res.resume();
                    res.on('end', () => {
                        callback(err);
                    });
                }
                else {
                    callback(err);
                }
            }, {}, payload);
        }, (err) => {
            if (err) {
                callback(err);
            }
            else {
                callback();
            }
        });
    }
    /**
     * This sends a GET request with healthcheck path to sproxyd
     * @param {Object} log - The log from s3
     * @param {SproxydClient-healthcheckCallback} callback - callback
     * @returns {void}
     */
    healthcheck(log, callback) {
        const logger = log || this.createLogger();
        const currentBootstrap = this.getCurrentBootstrap();
        const req = {
            hostname: currentBootstrap[0],
            port: currentBootstrap[1],
            method: 'GET',
            path: '/metrics',
            headers: {
                'X-Scal-Request-Uids': logger.getSerializedUids(),
            },
            agent: this.httpAgent,
        };
        const request = _createRequest(req, logger, callback);
        request.end();
    }
}
exports.HDProxydClient = HDProxydClient;
