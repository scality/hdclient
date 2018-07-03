'use strict'; // eslint-disable-line strict
/* eslint-disable max-len */

/**
 * Test helpers, mock and so forth
 */

const assert = require('assert');
const nock = require('nock'); // HTTP API mocking
const fs = require('fs');
const stream = require('stream');

const { protocol, keyscheme } = require('../index');

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
 * @param {String} endpoint (ip:port) to contact
 * @param {Object} keyContext same as given to actual PUT
 * @param {Number} statusCode of the reply
 * @param {fs.ReadStream|String} payload to return
 * @param {String} contentType (only 'data' supported as of now)
 * @param {Number} timeoutMs Delay reply by X ms
 * @return {Nock.Scope} can be used to further chain mocks
 *                      onto same machine
 */
function _mockPutRequest(endpoint, keyContext, statusCode,
                         payload, contentType, timeoutMs = 0) {
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
    return nock(`http://${endpoint}`, { reqheaders })
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
 *          - 0 => HTTP status code to return
 *          - 1 => payload (file system stream or string)
 *          - 2 => payload type ('data', 'usermd', etc) - only data for now
 *          - 3 => {Number} timeout ms
 * @returns {[Nock.Scope]} nock mocks
 *
 * @comment Endpoints are mocked in order. If you have more
 *          endpoints than reply, those will not be mocked.
 */
function mockPUT(clientConfig, keyContext, replies) {
    const mocks = replies.map((reply, idx) => {
        const [statusCode, payload, contentType, timeoutMs] = reply;
        const endpoint = clientConfig.endpoints[idx];
        return _mockPutRequest(endpoint, keyContext, statusCode,
                               payload, contentType, timeoutMs);
    });

    return mocks;
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
 *          - timeoutMs => {Number} timeout ms
 *          - range => {undefined | [Number]} range to return
 * @returns {[String, [Nock.Scope], [Nock.Scope]]} rawkey,
 *          data mocks and coding mocks
 *
 * @comment replies.length must be equal to number of parts
 */
function mockGET(clientConfig, objectKey, replies) {
    const nParts = clientConfig.dataParts +
          clientConfig.codingParts;
    const parts = keyscheme.keygen(
        clientConfig.endpoints,
        objectKey,
        clientConfig.dataParts,
        clientConfig.codingParts
    );

    assert.strictEqual(nParts, replies.length);

    // Setup data mocks
    const dataMocks = parts.data.map(
        (part, idx) => _mockGetRequest(part, replies[idx])
    );

    // Setup coding mocks
    const codingMocks = parts.coding.map(
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
function _mockDeleteRequest(location, statusCode, timeoutMs = 0) {
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
 * @comment each entry of replies must be an Array with:
 *          - 0 => {Number} HTTP status code to return
 *          - 1 => {Number} timeout ms
 * @return {[String, [Nock.Scope], [Nock.Scope]]} rawkey,
 *          data mocks and coding mocks
 *
 * @comment replies.length must be equal to number of parts
 */
function mockDELETE(clientConfig, objectKey, replies) {
    const nParts = clientConfig.dataParts +
          clientConfig.codingParts;
    const parts = keyscheme.keygen(
        clientConfig.endpoints,
        objectKey,
        clientConfig.dataParts,
        clientConfig.codingParts
    );

    assert.strictEqual(nParts, replies.length);

    // Setup data mocks
    const dataMocks = parts.data.map((part, idx) => {
        const [statusCode, timeoutMs] = replies[idx];
        return _mockDeleteRequest(part, statusCode, timeoutMs);
    });

    // Setup coding mocks
    const codingMocks = parts.coding.map((part, idx) => {
        const [statusCode, timeoutMs] = replies[idx + clientConfig.dataParts];
        return _mockDeleteRequest(part, statusCode, timeoutMs);
    });

    return [keyscheme.serialize(parts), dataMocks, codingMocks];
}

module.exports = {
    streamString,
    getPayloadLength,
    mockGET,
    mockPUT,
    mockDELETE,
};
