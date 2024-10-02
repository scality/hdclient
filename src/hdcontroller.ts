'use strict';  

import * as assert from 'assert';
import * as async from 'async';
import * as http from 'http';
import * as werelogs from 'werelogs';

import { http as httpAgent } from 'httpagent';
import { Stream } from 'stream';
import { shuffle } from './shuffle';


export class HDProxydError extends Error {
    public code: number | string | undefined;
    public isExpected: boolean = false;
}

type HDProxydCallback = (error?: HDProxydError, res?: http.IncomingMessage) => void;

type HDProxydClientPutCallback = (error?: HDProxydError, key?: string) => void;
type HDProxydClientGetCallback = (error?: HDProxydError, res?: Stream) => void;
type HDProxydClientDeleteCallback = (error?: HDProxydError) => void;

type Params = { [key: string]: string } & { range?: number[] };

/*
 * This handles the request, and the corresponding response default behaviour
 */
function _createRequest(req: http.RequestOptions, log: werelogs.RequestLogger, 
    callback: HDProxydCallback): http.ClientRequest {
    let callbackCalled = false;
    const request = http.request(req, response => {
        callbackCalled = true;
        // Get range returns a 206
        // Concurrent deletes on hdproxyd/immutable keys returns 423
        if (response.statusCode !== 200 && response.statusCode !== 206 &&
            !(response.statusCode === 423 && req.method === 'DELETE')) {
            const error = new HDProxydError();
            error.code = response.statusCode;
            error.isExpected = true;
            log.debug('got expected response code:',
                { statusCode: response.statusCode });
            response.resume(); // Drain the response stream
            return callback(error);
        }
        return callback(undefined, response);
    }).on('error', (err: HDProxydError) => {
        if (!callbackCalled) {
            callbackCalled = true;
            return callback(err);
        }
        if (err.code !== 'ERR_SOCKET_TIMEOUT') {
            log.error('got socket error after response', { err });
        }
        return null;
    });

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
function _parseBootstrapList(list: string[]): string[][] {
    return list.map(value => value.split(':'));
}

// tslint:disable-next-line: interface-name
export interface HDProxydOptions {
    bootstrap: string[];
    logApi: typeof werelogs;
}

// tslint:disable-next-line: interface-name
interface Headers {
    [key: string]: string;
}

export class HDProxydClient {
    private path: string;
    public bootstrap: string[][];
    private httpAgent: http.Agent;
    private logging!: werelogs.Logger;
    private current: string[] = ['', ''];
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
    constructor(opts: HDProxydOptions) {
        const options = opts || {} as HDProxydOptions;
        this.bootstrap = opts.bootstrap === undefined ?
            [['localhost', '18888']] : _parseBootstrapList(opts.bootstrap);
        this.bootstrap = shuffle(this.bootstrap);
        this.path = '/store/';
        this.setCurrentBootstrap(this.bootstrap[0]);
        this.httpAgent = new httpAgent.Agent({
            freeSocketTimeout: 60 * 1000,
            timeout: 2 * 60 * 1000,
        }) as http.Agent;
        this.setupLogging(options.logApi);
    }

    /**
     * Destroy connections kept alive by the client
     *
     * @return {undefined}
     */
    public destroy(): void {
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
    private setupLogging(logApi: typeof werelogs): void {
        this.logging = new (logApi || werelogs).Logger('HDProxydClient');
    }

    private createLogger(reqUids?: string): werelogs.RequestLogger {
        return reqUids ?
            this.logging.newRequestLoggerFromSerializedUids(reqUids) :
            this.logging.newRequestLogger();
    }

    private _shiftCurrentBootstrapToEnd(log: werelogs.RequestLogger): HDProxydClient {
        const previousEntry = this.bootstrap.shift() as string[];
        this.bootstrap.push(previousEntry);
        const newEntry = this.bootstrap[0];
        this.setCurrentBootstrap(newEntry);

        log.debug(`bootstrap head moved from ${previousEntry} to ${newEntry}`);
        return this;
    }

    public setCurrentBootstrap(host: string[]): HDProxydClient {
        this.current = host;
        return this;
    }

    public getCurrentBootstrap(): string[] {
        return this.current;
    }
    /**
     * Returns the first id from the array of request ids.
     * @param {Object} log - log from s3
     * @returns {String} - first request id
     */
    private _getFirstReqUid(log: werelogs.RequestLogger): string {
        let reqUids: string[] = [];

        if (log) {
            reqUids = log.getUids();
        }
        return reqUids[0];
    }

    /*
     * This creates a default request for hdproxyd
     */
    private _createRequestHeader(method: string, headers: {[key: string]: string}|undefined,
        key: string, params: Params, log: werelogs.RequestLogger): object {
        const reqHeaders = headers || {};

        const currentBootstrap: string[] = this.getCurrentBootstrap();
        const reqUids = this._getFirstReqUid(log);

        reqHeaders['content-type'] = 'application/octet-stream';
        reqHeaders['X-Scal-Request-Uids'] = reqUids;
        reqHeaders['X-Scal-Trace-Ids'] = reqUids;
        if (params && params.range) {
             
            reqHeaders.Range = `bytes=${params.range[0]}-${params.range[1]}`;
             
        }
        let realPath: string;
        if (key === '/job/delete') {
            realPath = key;
        } else {
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

    private _failover(method: string, stream: Stream|null, size: number, key: string,
        tries: number, log: werelogs.RequestLogger, callback: HDProxydCallback,  params?: Params,
        payload?: object): void {
        const args: Params = params === undefined ? {} : params;
        let counter = tries;
        log.debug('sending request to hdproxyd', { method, key, args, counter });

        let receivedResponse = false;
        this._handleRequest(method, stream, size, key, log, (err?: HDProxydError, ret?: http.IncomingMessage) => {
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
                    log.errorEnd('failover tried too many times, giving up',
                        { retries: counter });
                    return callback(err);
                }
                return this._shiftCurrentBootstrapToEnd(log)
                    ._failover(method, stream, size, key, counter, log,
                        callback, params);
            }
            receivedResponse = true;
            log.debug('request received response');
            return callback(err, ret);
        }, args, payload);
    }

    /*
     * This does a basic routing of the methods, dealing with the request
     * creation and its sending.
     */
    private _handleRequest(method: string, stream: Stream|null,
        size: number, key: string, log: werelogs.RequestLogger,
        callback: HDProxydCallback, params: Params,
        payload: object | undefined): void {
        //tslint:disable-next-line:no-any
        const headers = ( params.headers ? params.headers : {}) as { 'content-length'?: number; [key: string]: any };
        const req = this._createRequestHeader(method, headers, key, params,
            log);
        const host = this.getCurrentBootstrap();
        const isBatchDelete = key === '/job/delete';
        if (stream) {
            headers['content-length'] = size;
            const request = _createRequest(req, log, (err?: HDProxydError, response?: http.IncomingMessage) => {
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
            request.on('finish',
                () => log.debug('finished sending PUT chunks to hdproxyd', {
                    component: 'sproxydclient',
                    method: '_handleRequest',
                    contentLength: size,
                }));
            stream.pipe(request);
            stream.on('error', err => {
                log.error('error from readable stream', {
                    error: err,
                    method: '_handleRequest',
                    component: 'sproxydclient',
                });
                request.end();
            });
        } else {
            headers['content-length'] = isBatchDelete ? size : 0;
            const request = _createRequest(req, log, (err?: HDProxydError, response?: http.IncomingMessage) => {
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
    public put(stream: Stream, size: number, params: { [key: string]: string }, reqUids: string,
        callback: HDProxydClientPutCallback): void {
        const log = this.createLogger(reqUids);
        this._failover('POST', stream, size, '', 0, log, (err?: HDProxydError, response?: http.IncomingMessage) => {
            if (response) {
                response.resume();
            }
            if (err || !response) {
                return callback(err);
            }
            if (!response.headers['scal-key']) {
                return callback(new HDProxydError('no key returned'));
            }
            const key = response.headers['scal-key'] as string;
            response.on('end', () => callback(undefined, key));
            return null;
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
    public get(key: string, range: number[], reqUids: string, callback: HDProxydClientGetCallback): void {
        assert.strictEqual(typeof key, 'string');
        const log = this.createLogger(reqUids);
        const params = { range };
        this._failover('GET', null, 0, key, 0, log, callback, params as Params);
    }

    /**
     * This sends a DELETE request to hdproxyd.
     * @param {String} key - The key associated to the values
     * @param {String} reqUids - The serialized request id
     * @param {HDProxydClient~deleteCallback} callback - callback
     * @returns {undefined}
     */
    public delete(key: string, reqUids: string, callback: HDProxydClientDeleteCallback): void {
        assert.strictEqual(typeof key, 'string');
        const log = this.createLogger(reqUids);
        this._failover('DELETE', null, 0, key, 0, log, (err?: HDProxydError, res?: http.IncomingMessage) => {
            if (res) {
                // Drain the stream
                res.resume();
                res.on('end', () => {
                    callback(err);
                });
            } else {
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
    public batchDelete(list: {keys: string[]}, reqUids: string, callback: HDProxydClientDeleteCallback): void {
        assert.strictEqual(typeof list, 'object');
        assert(list.keys.every(k => typeof k === 'string'));
        // split the list into batches of 1000 each
        const batches : { keys: string[] }[] = [];
        while (list.keys.length > 0) {
            batches.push({ keys: list.keys.splice(0, 1000) });
        }
        async.eachLimit(batches, 5, (b, done) => {
            const log = this.createLogger(reqUids);
            const payload = Buffer.from(JSON.stringify(b.keys));
            this._failover('POST', null, payload.length, '/job/delete', 0,
                log, (err?: HDProxydError, res?: http.IncomingMessage) => {
                    if (res) {
                        // Drain the stream
                        res.resume();
                        res.on('end', () => {
                            done(err);
                        });
                    } else {
                        done(err);
                    }
                }, {}, payload);
        }, (err: undefined|null|HDProxydError) => {
            if (err) {
                callback(err);
            } else {
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
    public healthcheck(log: werelogs.RequestLogger, callback: HDProxydCallback): void {
        const logger = log || this.createLogger();
        const currentBootstrap = this.getCurrentBootstrap();
        const req = {
            hostname: currentBootstrap[0],
            port: currentBootstrap[1],
            method: 'GET',
            path: '/metrics', // XXX
            headers: {
                'X-Scal-Request-Uids': logger.getSerializedUids(),
            },
            agent: this.httpAgent,
        };
        const request = _createRequest(req, logger, callback);
        request.end();
    }
}
