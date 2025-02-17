import * as http from 'http';
import * as werelogs from 'werelogs';
import { Stream } from 'stream';
export declare class HDProxydError extends Error {
    code: number | string | undefined;
    isExpected: boolean;
}
type HDProxydCallback = (error?: HDProxydError, res?: http.IncomingMessage) => void;
type HDProxydClientPutCallback = (error?: HDProxydError, key?: string) => void;
type HDProxydClientGetCallback = (error?: HDProxydError, res?: Stream) => void;
type HDProxydClientDeleteCallback = (error?: HDProxydError) => void;
export interface HDProxydOptions {
    bootstrap: string[];
    logApi: typeof werelogs;
}
export declare class HDProxydClient {
    private path;
    bootstrap: string[][];
    private httpAgent;
    private logging;
    private current;
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
    constructor(opts: HDProxydOptions);
    /**
     * Destroy connections kept alive by the client
     *
     * @return {undefined}
     */
    destroy(): void;
    private setupLogging;
    private createLogger;
    private _shiftCurrentBootstrapToEnd;
    setCurrentBootstrap(host: string[]): HDProxydClient;
    getCurrentBootstrap(): string[];
    /**
     * Returns the first id from the array of request ids.
     * @param {Object} log - log from s3
     * @returns {String} - first request id
     */
    private _getFirstReqUid;
    private _createRequestHeader;
    private _failover;
    private _handleRequest;
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
    put(stream: Stream, size: number, params: {
        [key: string]: string;
    }, reqUids: string, callback: HDProxydClientPutCallback): void;
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
    get(key: string, range: number[], reqUids: string, callback: HDProxydClientGetCallback): void;
    /**
     * This sends a DELETE request to hdproxyd.
     * @param {String} key - The key associated to the values
     * @param {String} reqUids - The serialized request id
     * @param {HDProxydClient~deleteCallback} callback - callback
     * @returns {undefined}
     */
    delete(key: string, reqUids: string, callback: HDProxydClientDeleteCallback): void;
    /**
     * This sends a BATCH DELETE request to hdproxyd.
     * @param {Object} list - object containing a list of keys to delete
     * @param {Array} list.keys - array of string keys to delete
     * @param {String} reqUids - The serialized request id
     * @param {SproxydClient~deleteCallback} callback - callback
     * @returns {void}
     */
    batchDelete(list: {
        keys: string[];
    }, reqUids: string, callback: HDProxydClientDeleteCallback): void;
    /**
     * This sends a GET request with healthcheck path to sproxyd
     * @param {Object} log - The log from s3
     * @param {SproxydClient-healthcheckCallback} callback - callback
     * @returns {void}
     */
    healthcheck(log: werelogs.RequestLogger, callback: HDProxydCallback): void;
}
export {};
