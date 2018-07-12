'use strict'; // eslint-disable-line strict
/* eslint-disable max-len */

/**
 * Test helpers, mock and so forth
 */

const assert = require('assert');
const nock = require('nock'); // HTTP API mocking
const fs = require('fs');
const stream = require('stream');

const { hdclient, protocol, keyscheme, placement } = require('../index');

/**
 * Get object mocking HyperdriveClient errorAgent
 *
 * @return {Object} mocked agent
 */
function getMockedErrorAgent() {
    return {
        nextError: null,
        logged: new Map(),
        send(payloads, cb) {
            payloads.forEach(payload => {
                const cur = this.logged.has(payload.topic) ?
                          this.logged.get(payload.topic) :
                          [];
                this.logged.set(payload.topic,
                                cur.concat(payload.messages));
            });
            const ret = cb(this.nextError);
            this.netxtError = null;
            return ret;
        },
    };
}

/**
 * Helper to create a HyperdriveClient
 *
 * @param {Number} nLocations - how many hyperdrive availables
 * @param {String} code - Erasure coding or replication
 * @param {Number} nData - Number of data fragments
 * @param {Number} nCoding - Number of coding fragments
 * @return {HyperdriveClient} created client
 */
function getDefaultClient({ nLocations = 1,
                            code = 'CP',
                            nData = 1,
                            nCoding = 0 } = {}) {
    const conf = {
        code,
        dataParts: nData,
        codingParts: nCoding,
        requestTimeoutMs: 10,
        policy: {
            locations: [...Array(nLocations).keys()].map(
                idx => `hyperdrive-store-${idx}:8888`),
        },
    };

    hdclient.HyperdriveClient.prototype.setupErrorAgent = function mockedSetup() {
        this.errorAgent = getMockedErrorAgent();
    };

    hdclient.HyperdriveClient.prototype.destroyErrorAgent = function mockedDestroy() {
        this.errorAgent = null;
    };

    const client = new hdclient.HyperdriveClient(conf);
    // Deactivate all logs
    client.logging.config.update({ level: 'fatal', dump: 'fatal' });
    return client;
}

/**
 * Get content of a topic
 *
 * @param {HyperdriveClient} client - Client used
 * @param {String} topic - Topic to retrieve
 * @return {undefined} if topic does not exist
 * @return {[Object]} array of logged objects
 */
function getTopic(client, topic) {
    const content = client.errorAgent.logged.get(topic);
    if (content === undefined) {
        return content;
    }

    return content.map(log => JSON.parse(log));
}

/**
 * Create a readable stream from a buffer/string
 *
 * @param {String|Buffer} buffer to stream
 * @return {stream.Readable} readable stream
 */
function streamString(buffer) {
    const streamed = new stream.Readable();
    streamed.push(buffer);
    streamed.push(null);
    return streamed;
}

/**
 * Retrieve payload length
 *
 * GET replies must fill the Conten-Length header
 * @param {fs.ReadStream|String} payload to size
 * @returns {Number} payload's length
 * @comment Blocking! Use for test purposes only
 */
function getPayloadLength(payload) {
    if (payload instanceof fs.ReadStream) {
        const stat = fs.statSync(payload.path);
        return stat.size;
    }

    return payload.length;
}

/**
 * Mock a single PUT call on a given host:port
 *
 * @param {String} location (ip:port) to contact
 * @param {Object} keyContext same as given to actual PUT
 * @param {Number} statusCode of the reply
 * @param {fs.ReadStream|String} payload to return
 * @param {String} contentType (only 'data' supported as of now)
 * @param {Number} timeoutMs Delay reply by X ms
 * @return {Nock.Scope} can be used to further chain mocks
 *                      onto same machine
 */
function _mockPutRequest(location, keyContext,
                         { statusCode, payload, contentType, timeoutMs = 0 }) {
    const len = getPayloadLength(payload);
    const reqheaders = {
        ['Content-Length']: len,
        ['Content-Type']: protocol.helpers.makePutContentType(
            { [contentType]: len }),
    };

    const replyheaders = {
        ['Content-Type']:
        `${protocol.specs.HYPERDRIVE_APPLICATION}; ${contentType}=${len}; $crc.${contentType}=0xdeadbeef`,
    };

    const keyPrefix = keyContext.objectKey.slice(0, 8);
    const expectedPathPrefix =
              `${protocol.specs.STORAGE_BASE_URL}/${keyPrefix}`;

    // TODO find a better filtering solution...
    return nock(`http://${location}`, { reqheaders })
        .filteringPath(path => {
            if (path.startsWith(expectedPathPrefix)) {
                return `${protocol.specs.STORAGE_BASE_URL}/defaultkey`;
            }
            return path;
        })
        .put(`${protocol.specs.STORAGE_BASE_URL}/defaultkey`)
        .delay(timeoutMs)
        .reply(statusCode, '', replyheaders);
}

/**
 * Mock an object-level singel PUT call
 *
 * This function can mock PUT for all parts,
 * and return specific codes for each.
 * /!\ ALL ENDPOINTS ARE MOCKED, you must have a setup
 * that enables you to know which are going to contacted
 *
 * @param {Object} clientConfig Hyperdrive client configuration
 * @param {String} keyContext same as given to actual PUT
 * @param {[Reply]} replies description
 * @comment each entry of replies must be an Array with:
 *          - statusCode => {Number} HTTP status code to return
 *          - payload {fs.ReadStream | String } body to match (only match size for now)
 *          - contentType ('data', 'usermd', etc) - only data for now
 *          [- timeoutMs] => {Number} timeout ms
 * @return {Object} with dataMocks and codingMocks keys
 */
function mockPUT(clientConfig, keyContext, replies) {
    const { dataLocations, codingLocations } = placement.select(
        clientConfig.policy,
        clientConfig.dataParts,
        clientConfig.codingParts
    );

    assert.strictEqual(replies.length,
                       dataLocations.length + codingLocations.length);

    const dataMocks = dataLocations.map(
        (loc, idx) => _mockPutRequest(loc, keyContext, replies[idx])
    );

    const codingMocks = codingLocations.map(
        (loc, idx) => _mockPutRequest(loc, keyContext,
                                      replies[dataLocations.length + idx])
    );

    return { dataMocks, codingMocks };
}

/**
 * Mock a single GET call on a given host:port
 *
 * @param {Object} location infos
 * @param {String} location.hostname to contact
 * @param {Number} location.port to contact
 * @param {String} location.key to retrieve
 * @param {Number} statusCode of the reply
 * @param {fs.ReadStream|String} payload to return
 * @param {String} acceptType (only 'data' supported as of now)
 * @param {Number} timeoutMs Delay reply by X ms
 * @return {Nock.Scope} can be used to further chain mocks
 *                      onto same machine
 */
function _mockGetRequest(location,
                         { statusCode, payload, acceptType, timeoutMs = 0, range }) {
    const endpoint = `http://${location.hostname}:${location.port}`;
    let content = payload;
    let len = getPayloadLength(payload);
    if (typeof(payload) === 'string' && range) {
        if (range.length === 1) {
            content = payload.slice(range[0]);
        } else {
            // Slice does not include right boundary, while HTTP ranges are inclusive
            content = payload.slice(range[0], range[1] + 1);
        }
        len = content.length;
    }

    const reqheaders = {
        ['Accept']: protocol.helpers.makeAccept([acceptType, range]),
    };
    protocol.specs.GET_QUERY_MANDATORY_HEADERS.forEach(
        header => assert.ok(reqheaders[header] !== undefined)
    );

    /* Set Content-Length iff a valid answer is expected */
    const replyheaders = statusCode === 200 ? {
        ['Content-Length']: len,
    } : {};

    const path = `${protocol.specs.STORAGE_BASE_URL}/${location.key}`;

    return nock(endpoint, { reqheaders })
        .get(path)
        .delay(timeoutMs)
        .reply(statusCode, content, replyheaders);
}

/**
 * Mock an object-level singel GET call
 *
 * This function can mock GET for all parts,
 * and return specific codes for each.
 * It generates a rawKey (as if there was a PUT before).
 *
 * @param {Object} clientConfig Hyperdrive client configuration
 * @param {String} objectKey The object identifier
 * @param {[Object]} replies description
 * @comment each entry of replies must be an Object with:
 *          - statusCode => {Number} HTTP status code to return
 *          - payload => {String|fs.ReadStream} payload (file system stream or string)
 *          - acceptType => {String} payload type ('data', 'usermd', etc)
 *          [- timeoutMs] => {Number} timeout ms
 *          - range => {undefined | [Number]} range to return
 * @comment replies.length must be equal to number of parts
 * @return {Object} with rawKey, dataMocks and codingMocks keys
 */
function mockGET(clientConfig, objectKey, replies) {
    const nParts = clientConfig.dataParts +
          clientConfig.codingParts;
    const parts = keyscheme.keygen(
        clientConfig.policy,
        objectKey,
        getPayloadLength(replies[0].payload),
        'CP',
        clientConfig.dataParts,
        clientConfig.codingParts
    );

    assert.strictEqual(nParts, replies.length);

    // Setup data mocks
    const dataMocks = parts.chunks[0].data.map(
        (part, idx) => _mockGetRequest(part, replies[idx])
    );

    // Setup coding mocks
    const codingMocks = parts.chunks[0].coding.map(
        (part, idx) => _mockGetRequest(part, replies[idx])
    );

    return [keyscheme.serialize(parts), dataMocks, codingMocks];
}

/**
 * Mock a single DELETE call on a given host:port
 *
 * @param {Object} location infos
 * @param {String} location.hostname to contact
 * @param {Number} location.port to contact
 * @param {String} location.key to retrieve
 * @param {Number} statusCode of the reply
 * @param {Number} timeoutMs Delay reply by X ms
 * @return {Nock.Scope} can be used to further chain mocks
 *                      onto same machine
 */
function _mockDeleteRequest(location, { statusCode, timeoutMs = 0 }) {
    const endpoint = `http://${location.hostname}:${location.port}`;

    const reqheaders = {
        ['Accept']: protocol.helpers.makeAccept(),
        ['Content-Length']: 0,
    };
    protocol.specs.DELETE_QUERY_MANDATORY_HEADERS.forEach(
        header => assert.ok(reqheaders[header] !== undefined)
    );

    const replyheaders = {
        ['Content-Length']: 0,
    };
    protocol.specs.DELETE_REPLY_MANDATORY_HEADERS.forEach(
        header => assert.ok(replyheaders[header] !== undefined)
    );

    const path = `${protocol.specs.STORAGE_BASE_URL}/${location.key}`;

    return nock(endpoint, { reqheaders })
        .delete(path)
        .delay(timeoutMs)
        .reply(statusCode, '', replyheaders);
}

/**
 * Mock an object-level singel DELETE call
 *
 * This function can mock DELETE for all parts,
 * and return specific codes for each.
 * It generates a rawKey (as if there was a PUT before).
 *
 * @param {Object} clientConfig Hyperdrive client configuration
 * @param {String} objectKey The object identifier
 * @param {[Reply]} replies description
 * @comment each entry of replies must be an Object with:
 *          - statusCode => {Number} HTTP status code to return
 *          [- timeoutMS] => {Number} timeout ms
 * @comment replies.length must be equal to number of parts
 * @return {Object} with rawKey, dataMocks and codingMocks keys
 */
function mockDELETE(clientConfig, objectKey, replies) {
    const nParts = clientConfig.dataParts +
          clientConfig.codingParts;
    const parts = keyscheme.keygen(
        clientConfig.policy,
        objectKey,
        1024,
        clientConfig.code,
        clientConfig.dataParts,
        clientConfig.codingParts
    );

    assert.strictEqual(nParts, replies.length);

    // Setup data mocks
    const dataMocks = parts.chunks[0].data.map(
        (part, idx) => _mockDeleteRequest(part, replies[idx])
    );

    // Setup coding mocks
    const codingMocks = parts.chunks[0].coding.map(
        (part, idx) => _mockDeleteRequest(part, replies[idx + clientConfig.dataParts])
    );

    return { rawKey: keyscheme.serialize(parts), dataMocks, codingMocks };
}

module.exports = {
    getDefaultClient,
    streamString,
    getPayloadLength,
    getTopic,
    mockGET,
    mockPUT,
    mockDELETE,
};
