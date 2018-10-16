'use strict'; // eslint-disable-line strict
/* eslint-disable max-len */

/**
 * Test helpers, mock and so forth
 */

const assert = require('assert');
const nock = require('nock'); // HTTP API mocking
const fs = require('fs');
const stream = require('stream');

const { hdclient, protocol, keyscheme, split,
        utils: libUtils } = require('../index');


/* Override placement policy for determinism in tests */
function deterministicPlacement(policy, nData, nCoding) {
    const len = policy.locations.length;
    let pos = 0;

    const dataLocations = [];
    for (let i = 0; i < nData; ++i) {
        dataLocations.push(policy.locations[pos]);
        pos = (pos + 1) % len;
    }

    const codingLocations = [];
    for (let i = 0; i < nCoding; ++i) {
        codingLocations.push(policy.locations[pos]);
        pos = (pos + 1) % len;
    }

    return { dataLocations, codingLocations };
}

keyscheme.updateLocationSelector(deterministicPlacement);


/**
 * Get object mocking HyperdriveClient errorAgent
 *
 * @return {Object} mocked agent
 */
function getMockedErrorAgent() {
    return {
        nextError: null,
        logged: new Map(),
        produce(topic, partition, message) {
            return new Promise((resolve, reject) => {
                if (this.nextError) {
                    const err = this.nextError;
                    this.netxtError = null;
                    reject(err);
                    return;
                }

                const cur = this.logged.has(topic) ?
                          this.logged.get(topic) : [];
                cur.push(message);
                this.logged.set(topic, cur);
                resolve();
            });
        },
    };
}

/**
 * Strict, deep comparison of topics
 *
 * @param {Object} realContent - What we have
 * @param {Object} expectedContent - What we want
 * @return {undefined}
 * @throw {AssertionError} if anything is fishy
 */
function strictCompareTopicContent(realContent, expectedContent) {
    assert.deepStrictEqual(realContent, expectedContent);
}

/**
 * Helper to create a HyperdriveClient
 *
 * @param {Number} nLocations - how many hyperdrive availables
 * @param {Object[]|null} codes - Code config section (default:null => CP,1)
 * @param {String} codes[].type - Type of code to use
 * @param {Number} codes[].dataParts - Number of data fragments
 * @param {Number} codes[].codingParts - Number of coding fragments
 * @param {String} codes[].pattern - Regex to match on bucket/object name
 * @param {Number} nData - Number of data fragments
 * @param {Number} nCoding - Number of coding fragments
 * @return {HyperdriveClient} created client
 */
function getDefaultClient({ nLocations = 1,
                            codes = null,
                            minSplitSize = 0 } = {}) {
    const defaultCodes = [{ type: 'CP', dataParts: nLocations, codingParts: 0, pattern: '.*' }];
    const codesToUse = codes || defaultCodes;
    const locations = libUtils.range(nLocations).map(
        idx => `uuid-${idx}`);
    const uuidmapping = {};
    locations.forEach(
        (uuid, idx) => {
            uuidmapping[uuid] = `fake-hyperdrive-${idx}:8888`;
        });
    const conf = {
        codes: codesToUse,
        requestTimeoutMs: 10,
        policy: { minSplitSize, locations },
        uuidmapping,
        errorAgent: { kafkaBrokers: 'who cares?' },
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
 * @return {Object[]} array of logged objects
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
 * GET replies must fill the Content-Type header
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
 * Retrieve Content Length (can include trailing CRCs)
 *
 * GET replies must fill the Content-Length header
 * @param {Number} size of payload (see getPayloadLength)
 * @param {null|Number[]} range requested
 * @returns {Number} content's length
 * @comment Blocking! Use for test purposes only
 */
function getContentLength(size, range) {
    return range ? size : size + 12; /* 3 * 4 bytes CRC32 */
}

/**
 * Get body to return (potentially adding CRCs)
 *
 * @param {fs.ReadStream|Buffer} payload to size
 * @param {null|Number[]} range requested
 * @param {String} trailingCRCs to append
 * @returns {fs.ReadStream|Buffer} body to return
 */
function getReturnedBody(payload, range, trailingCRCs) {
    if (range) {
        return payload;
    }

    if (payload instanceof fs.ReadStream) {
        /* Concat payload with trailingCRCs
         * Identity transform, and on 'end'
         * event append the CRCs, then end.
         */
        const content = new stream.PassThrough();
        payload.pipe(content, { end: false });
        payload.once('end', () => {
            content.write(trailingCRCs);
            content.end();
        });
        return content;
    }

    return Buffer.concat([Buffer.from(payload), trailingCRCs]);
}

/**
 * Retrieve expected mocked request body
 *
 * PUT replies must fill the ContenT-Length header
 * @param {fs.ReadStream|String} payload to size
 * @returns {Number} payload's length
 * @comment Blocking! Use for test purposes only
 */
function getExpectedBody(payload) {
    if (payload instanceof fs.ReadStream) {
        return fs.readFileSync(payload.path);
    }

    return payload;
}

/**
 * Mock a single PUT call on a given host:port
 *
 * @param {Number} serviceId ServiceId of key creator
 * @param {Map} uuidmapping Map UUIDS to hyperdrive endpoints (ip:port)
 * @param {String} uuid  to contact
 * @param {Object} keyContext same as given to actual PUT
 * @param {String} endOffset Total end offset of this chunk in object
 * @param {Number} fragmentId Index of fragment
 * @param {Number} statusCode of the reply
 * @param {fs.ReadStream|String} payload to return
 * @param {String} contentTybpe (only 'data' supported as of now)
 * @param {Number} timeoutMs Delay reply by X ms
 * @return {Nock.Scope} can be used to further chain mocks
 *                      onto same machine
 */
function _mockPutRequest(serviceId, uuidmapping, uuid, keyContext, endOffset, fragmentId,
                         { statusCode, payload, contentType, timeoutMs = 0 }) {
    const len = getPayloadLength(payload);
    const expectedBody = getExpectedBody(payload);
    const reqheaders = {
        ['Content-Length']: len,
        ['Content-Type']: protocol.helpers.makePutContentType(
            { [contentType]: len }),
    };

    const replyheaders = {
        ['Content-Type']:
        `${protocol.specs.HYPERDRIVE_APPLICATION}; ${contentType}=${len}; $crc.${contentType}=0xdeadbeef`,
    };

    const hash = keyscheme.keyhash(keyContext);
    const expectedKeyPattern = [
        serviceId, '[0-9]+', hash,
        endOffset, fragmentId,
    ].join(keyscheme.PART_KEY_SEPARATOR);
    const mockedPathRegex = new RegExp(`${protocol.specs.STORAGE_BASE_URL}/${expectedKeyPattern}`);
    const { hostname, port } = libUtils.resolveUUID(uuidmapping, uuid);

    return nock(`http://${hostname}:${port}`, { reqheaders })
        .put(mockedPathRegex, body =>
            /* Stupid nock forces to match expected body against
             * a stringified buffer, of which I can't specify the encoding...
             */
            body === expectedBody.toString('hex') ||
                body === expectedBody.toString())
        .delay(timeoutMs)
        .reply(statusCode, '', replyheaders);
}

// For some reason, JSDoc is happy with this syntax, but not ESLInt...
/* eslint-disable valid-jsdoc */
/**
 * Mock an object-level singel PUT call
 *
 * This function can mock PUT for all parts,
 * and return specific codes for each.
 * /!\ ALL ENDPOINTS ARE MOCKED, you must have a setup
 * that enables you to know which are going to be contacted
 *
 * @param {HyperdriveClient} client - Hyperdrive client instance
 * @param {Object} keyContext - Same as given to actual PUT { objectKey, bucketName, versionId }
 * @param {Number} size - Total payload size
 * @param {Object[][]} repliess - Description of all mocks
 * @param {Number} repliess[][].statuscode -  HTTP status code to return
 * @param {String|fs.ReadStream} repliess[][].payload - data to return
 * @param {String} repliess[][].contentType - payload type ('data', 'usermd', etc)
 * @param {undefined|Number} repliess[][].timeoutMs - timeout of the query
 * @comment replies.length must be equal to nChunks * nPparts
 * @return {Object} with rawKey and mocks
 * @comment mocks is an array of {dataMocks: [mock], codingMocks: [mock]}
 */
/* eslint-enable valid-jsdoc */
function mockPUT(client, keyContext, size, repliess) {
    const clientConfig = client.options;
    const code = client.selectCode(keyContext.bucketName, keyContext.objectKey);
    if (code === null) {
        return {};
    }

    const { splitSize } = split.getSplitSize(
        clientConfig.policy.minSplitSize, size, code.type, code.dataParts);
    const { dataLocations, codingLocations } = deterministicPlacement(
        clientConfig.policy, code.dataParts, code.codingParts);
    const nParts = dataLocations.length + codingLocations.length;
    assert.ok(repliess.every(c => c.length === nParts));

    const mocks = repliess.map((replies, chunkId) => {
        const endOffset = Math.min(size, splitSize * (chunkId + 1));
        const dataMocks = dataLocations.map(
            (loc, idx) => _mockPutRequest(
                clientConfig.serviceId,
                clientConfig.uuidmapping,
                loc, keyContext, endOffset, idx,
                replies[idx]));

        const codingMocks = codingLocations.map(
            (loc, idx) => _mockPutRequest(
                clientConfig.serviceId,
                clientConfig.uuidmapping,
                loc, keyContext, endOffset, dataLocations.length + idx,
                replies[dataLocations.length + idx]));

        return { dataMocks, codingMocks };
    });

    return { mocks };
}

/**
 * Mock a single GET call on a given host:port
 *
 * @param {Map} uuidmapping Map UUIDS to hyperdrive endpoints (ip:port)
 * @param {Object} location infos
 * @param {String} location.uuid to contact
 * @param {String} location.key to retrieve
 * @param {Number} statusCode of the reply
 * @param {fs.ReadStream|String} payload to return
 * @param {String} acceptType (only 'data' supported as of now)
 * @param {Number} timeoutMs Delay reply by X ms
 * @param {Number} storedCRC CRC retrieved from the index
 * @param {Number} actualCRC CRC computed by 'reading' the data
 * @return {Nock.Scope} can be used to further chain mocks
 *                      onto same machine
 */
function _mockGetRequest(uuidmapping,
                         location,
                         { statusCode, payload, acceptType,
                           timeoutMs = 0, range,
                           storedCRC = 0xdeadbeef,
                           actualCRC = 0xdeadbeef,
                         }) {
    if (statusCode === undefined) {
        return null;
    }
    const storedCRCstr = storedCRC.toString(16);
    /* Hyperdrive returns CRC as Little-Endian byte array... */
    const trailingCRCs = Buffer.alloc(12);
    trailingCRCs.writeUInt32LE(actualCRC);

    const { hostname, port } = libUtils.resolveUUID(uuidmapping, location.uuid);
    const endpoint = `http://${hostname}:${port}`;
    let content = payload;
    if (typeof(payload) === 'string' && range) {
        if (range.length === 1) {
            content = payload.slice(range[0]);
        } else {
            // Slice does not include right boundary, while HTTP ranges are inclusive
            content = payload.slice(range[0], range[1] + 1);
        }
    }
    const len = getPayloadLength(content);

    const reqheaders = {
        ['Accept']: protocol.helpers.makeAccept(
            [acceptType, range], ['crc']),
    };

    protocol.specs.GET_QUERY_MANDATORY_HEADERS.forEach(
        header => assert.ok(reqheaders[header] !== undefined)
    );

    const contentTypes = [`${protocol.specs.HYPERDRIVE_APPLICATION}`,
                          `data=${len}`];
    if (!range) {
        contentTypes.push('crc=12');
        contentTypes.push(`$crc.data=0x${storedCRCstr}`);
    }

    /* Set Content-Length iff a valid answer is expected */
    const replyheaders = statusCode === 200 ? {
        ['Content-Length']: getContentLength(len, range),
        ['Content-Type']: contentTypes.join('; '),
    } : {};

    const path = `${protocol.specs.STORAGE_BASE_URL}/${location.key}`;
    const returnedBody = getReturnedBody(content, range, trailingCRCs);
    return nock(endpoint, { reqheaders })
        .get(path)
        .delay(timeoutMs)
        .reply(statusCode, returnedBody, replyheaders);
}

// For some reason, JSDoc is happy with this syntax, but not ESLInt...
/* eslint-disable valid-jsdoc */
/**
 * Mock an object-level singel GET call
 *
 * This function can mock GET for all parts,
 * and return specific codes for each.
 * It generates a rawKey (as if there was a PUT before).
 *
 * @param {HyperdriveClient} client - Hyperdrive client instance
 * @param {Object} keyContext - Same as given to actual PUT { objectKey, bucketName, versionId }
 * @param {Number} objectSize - Total object size
 * @param {Object[][]} repliess - Description of all mocks
 * @param {Number} repliess[][].statuscode -  HTTP status code to return
 * @param {String|fs.ReadStream} repliess[][].payload - data to return
 * @param {String} repliess[][].acceptType - payload type ('data', 'usermd', etc)
 * @param {Number[]|undefined} repliess[][].range - HTTP range to return
 * @param {undefined|Number} repliess[][].timeoutMs - timeout of the query
 * @param {Number} repliess[][].storedCRC - CRC retrieved from the index
 * @param {Number} repliess[][].actualCRC - CRC computed by 'reading' the data
 * @comment replies.length must be equal to nChunks * nPparts
 * @return {Object} with rawKey and mocks
 * @comment mocks is an array of {dataMocks: [mock], codingMocks: [mock]}
 */
/* eslint-enable valid-jsdoc */
function mockGET(client, keyContext, objectSize, repliess) {
    const clientConfig = client.options;
    const nDataParts = clientConfig.codes[0].dataParts;
    const nCodingParts = clientConfig.codes[0].codingParts;
    const parts = keyscheme.keygen(
        clientConfig.serviceId,
        clientConfig.policy,
        keyContext,
        objectSize,
        clientConfig.codes[0].type,
        nDataParts,
        nCodingParts
    );

    assert.strictEqual(parts.nChunks, repliess.length);
    assert.ok(repliess.every(c => c.length === nDataParts + nCodingParts));

    const mocks = parts.chunks.map((chunk, chunkId) => {
        // Setup data mocks
        const dataMocks = chunk.data.map(
            (part, idx) => {
                const mockedReply = repliess[chunkId][idx];
                const { use, chunkRange } = libUtils.getChunkRange(parts, chunkId, mockedReply.range);
                assert.ok(use);
                mockedReply.range = chunkRange;
                return _mockGetRequest(clientConfig.uuidmapping, part, mockedReply);
            }
        );

        // Setup coding mocks
        const codingMocks = chunk.coding.map(
            (part, idx) => {
                const mockedReply = repliess[chunkId][nDataParts + idx];
                const { use, range } = libUtils.getChunkRange(parts, chunkId, mockedReply.range);
                assert.ok(use);
                mockedReply.range = range;
                return _mockGetRequest(clientConfig.uuidmapping, part, mockedReply);
            }
        );

        return { dataMocks, codingMocks };
    });

    return { rawKey: keyscheme.serialize(parts), mocks };
}

/**
 * Mock a single DELETE call on a given host:port
 *
 * @param {Map} uuidmapping Map UUIDS to hyperdrive endpoints (ip:port)
 * @param {Object} location infos
 * @param {String} location.uuid to contact
 * @param {String} location.key to retrieve
 * @param {Number} statusCode of the reply
 * @param {Number} timeoutMs Delay reply by X ms
 * @return {Nock.Scope} can be used to further chain mocks
 *                      onto same machine
 */
function _mockDeleteRequest(uuidmapping, location, { statusCode, timeoutMs = 0 }) {
    const { hostname, port } = libUtils.resolveUUID(uuidmapping, location.uuid);
    const endpoint = `http://${hostname}:${port}`;

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

// For some reason, JSDoc is happy with this syntax, but not ESLInt...
/* eslint-disable valid-jsdoc */
/**
 * Mock an object-level singel DELETE call
 *
 * This function can mock DELETE for all parts,
 * and return specific codes for each.
 * It generates a rawKey (as if there was a PUT before).
 *
 * @param {HyperdriveClient} client - Hyperdrive client instance
 * @param {Object} keyContext - Same as given to actual PUT { objectKey, bucketName, versionId }
 * @param {Number} objectSize - Total object size
 * @param {Object[][]} repliess - Description of all mocks
 * @param {Number} repliess[][].statusCode - HTTP status code to return
 * @param {Number|undefined} repliess[][].timeoutMs - timeout of the query
 * @comment replies are organized:  [replies] per chunk
 * @comment replies.length must be equal to nChunks * nPparts
 * @return {Object} with rawKey and mocks
 * @comment mocks is an array of {dataMocks: [mock], codingMocks: [mock]}
 */
/* eslint-enable valid-jsdoc */
function mockDELETE(client, keyContext, objectSize, repliess) {
    const clientConfig = client.options;
    const nDataParts = clientConfig.codes[0].dataParts;
    const nCodingParts = clientConfig.codes[0].codingParts;
    const parts = keyscheme.keygen(
        clientConfig.serviceId,
        clientConfig.policy,
        keyContext,
        objectSize,
        clientConfig.codes[0].type,
        nDataParts,
        nCodingParts
    );

    assert.strictEqual(parts.nChunks, repliess.length);
    assert.ok(repliess.every(c => c.length === nDataParts + nCodingParts));

    const mocks = parts.chunks.map((chunk, chunkId) => {
        // Setup data mocks
        const dataMocks = chunk.data.map(
            (part, idx) => _mockDeleteRequest(
                clientConfig.uuidmapping, part, repliess[chunkId][idx])
        );

        // Setup coding mocks
        const codingMocks = chunk.coding.map(
            (part, idx) => _mockDeleteRequest(
                clientConfig.uuidmapping, part,
                repliess[chunkId][idx + nDataParts])
        );

        return { dataMocks, codingMocks };
    });

    return { rawKey: keyscheme.serialize(parts), mocks };
}

module.exports = {
    getDefaultClient,
    strictCompareTopicContent,
    streamString,
    getPayloadLength,
    getTopic,
    mockGET,
    mockPUT,
    mockDELETE,
};
