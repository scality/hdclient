'use strict'; // eslint-disable-line strict
/* eslint-disable max-len */
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const assert = require('assert');
const crypto = require('crypto');
const fs = require('fs');
const mocha = require('mocha');
const nock = require('nock');
const stream = require('stream');
const ecstream = require('ecstream');

const hdclient = require('../../index');
const hdmock = require('../utils');

mocha.describe('PUT', function () {
    // Clean all HTTP mocks before starting the test
    mocha.beforeEach(nock.cleanAll);

    // Verify all mocks have been used - no garbage
    mocha.afterEach(function () { assert.ok(nock.isDone); });

    const deleteTopic = hdclient.httpUtils.topics.delete;
    const repairTopic = hdclient.httpUtils.topics.repair;
    const keyContext = {
        bucketName: 'testbucket',
        objectKey: 'best / :Obj~Ever!',
        versionId: 1,
    };


    mocha.describe('Single hyperdrive', function () {
        mocha.it('Success small key', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const content = 'Je suis une mite en pullover';
            const mocks = [[
                {
                    statusCode: 200,
                    payload: content,
                    contentType: 'data',
                },
            ]];

            hdmock.mockPUT(
                hdClient, keyContext,
                hdmock.getPayloadLength(content),
                mocks);

            hdClient.put(
                hdmock.streamString(content),
                hdmock.getPayloadLength(content),
                keyContext, '1',
                (err, rawKey) => {
                    assert.ifError(err);
                    /* Check generated key */
                    assert.strictEqual(typeof rawKey, 'string');
                    const parts = hdclient.keyscheme.deserialize(rawKey);

                    const uuid = hdClient.conf.policy.cluster.components[0].name;
                    assert.strictEqual(parts.nDataParts, 1);
                    assert.strictEqual(parts.nCodingParts, 0);
                    assert.strictEqual(parts.nChunks, 1);
                    const fragment = parts.chunks[0].data[0];
                    assert.strictEqual(fragment.uuid, uuid);

                    /* Check cleanup mechanism */
                    const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        delTopic, undefined);
                    const repTopic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        repTopic, undefined);
                    done();
                });
        });

        mocha.it('Success larger key (32 KiB)', function (done) {
            const hdClient = hdmock.getDefaultClient();
            /* TODO avoid depending on hardcoded path */
            const content = fs.createReadStream(
                'tests/functional/random_payload');
            const mocks = [[
                {
                    statusCode: 200,
                    payload: content,
                    contentType: 'data',
                },
            ]];

            hdmock.mockPUT(
                hdClient, keyContext,
                hdmock.getPayloadLength(content),
                mocks);

            hdClient.put(
                content,
                hdmock.getPayloadLength(content),
                keyContext, '1',
                (err, rawKey) => {
                    assert.ifError(err);
                    /* Check generated key */
                    assert.strictEqual(typeof rawKey, 'string');
                    const parts = hdclient.keyscheme.deserialize(rawKey);

                    const uuid = hdClient.conf.policy.cluster.components[0].name;
                    assert.strictEqual(parts.nDataParts, 1);
                    assert.strictEqual(parts.nCodingParts, 0);
                    assert.strictEqual(parts.nChunks, 1);
                    const fragment = parts.chunks[0].data[0];
                    assert.strictEqual(fragment.uuid, uuid);
                    assert.ok(fragment.key, keyContext.objectKey);

                    /* Check cleanup mechanism */
                    const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        delTopic, undefined);
                    const repTopic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        repTopic, undefined);
                    done();
                });
        });

        mocha.it('Server error', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const content = 'Je suis une mite en pullover';
            const mocks = [[
                {
                    statusCode: 500,
                    payload: content,
                    contentType: 'data',
                },
            ]];

            hdmock.mockPUT(
                hdClient, keyContext,
                hdmock.getPayloadLength(content),
                mocks);

            hdClient.put(
                hdmock.streamString(content),
                hdmock.getPayloadLength(content),
                keyContext, '1',
                (err, rawKey) => {
                    /* Check cleanup mechanism */
                    const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        delTopic,
                        [{
                            rawKey,
                            fragments: [[0, 0]],
                            version: 1,
                        }]);
                    const repTopic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        repTopic, undefined);

                    /* Check for errors */
                    assert.strictEqual(err.code,
                                       mocks[0][0].statusCode);
                    done();
                });
        });

        mocha.it('Timeout', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const content = 'Je suis une mite en pullover';
            const mocks = [[
                {
                    statusCode: 200,
                    payload: content,
                    contentType: 'data',
                    timeoutMs: hdClient.options.requestTimeoutMs + 10,
                },
            ]];

            hdmock.mockPUT(
                hdClient, keyContext,
                hdmock.getPayloadLength(content),
                mocks);

            let called = false;
            hdClient.put(
                hdmock.streamString(content),
                hdmock.getPayloadLength(content),
                keyContext, '1',
                (err, rawKey) => {
                    assert.ok(!called);
                    called = true;

                    /* Check cleanup mechanism */
                    const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                    const delLoggedErrors = [{
                        rawKey,
                        fragments: [[0, 0]],
                        version: 1,
                    }];
                    hdmock.strictCompareTopicContent(
                        delTopic, delLoggedErrors);
                    const repTopic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        repTopic, undefined);

                    /* Check for errors */
                    assert.strictEqual(err.code, 504);
                    assert.strictEqual(err.message, 'TimeoutError');
                    done();
                });
        });
    });

    mocha.describe('Multiple hyperdrives', function () {
        mocha.describe('Replication', function () {
            mocha.it('All Success', function (done) {
                const codes = [{ type: 'CP', dataParts: 3, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes });
                const content = 'Je suis une mite en pullover';
                const mocks = [[
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                    },
                ]];

                hdmock.mockPUT(
                    hdClient, keyContext,
                    hdmock.getPayloadLength(content),
                    mocks);

                hdClient.put(
                    hdmock.streamString(content),
                    hdmock.getPayloadLength(content),
                    keyContext, '1',
                    (err, rawKey) => {
                        assert.ifError(err);
                        /* Check generated key */
                        assert.strictEqual(typeof rawKey, 'string');
                        const parts = hdclient.keyscheme.deserialize(rawKey);
                        assert.strictEqual(parts.nDataParts, 3);
                        assert.strictEqual(parts.nCodingParts, 0);
                        assert.strictEqual(parts.nChunks, 1);

                        /* Check cleanup mechanism */
                        const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                        hdmock.strictCompareTopicContent(
                            delTopic, undefined);
                        const repTopic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            repTopic, undefined);

                        /* Check for errors */
                        done(err);
                    });
            });

            mocha.it('Single failure', function (done) {
                const codes = [{ type: 'CP', dataParts: 3, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes });
                const content = fs.createReadStream(
                    'tests/functional/random_payload');
                const mocks = [[
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                    },
                    {
                        statusCode: 500,
                        payload: content,
                        contentType: 'data',
                    },
                ]];

                hdmock.mockPUT(
                    hdClient, keyContext,
                    hdmock.getPayloadLength(content),
                    mocks);

                hdClient.put(
                    content,
                    hdmock.getPayloadLength(content),
                    keyContext, '1',
                    (err, rawKey) => {
                        /* Check for errors */
                        assert.ifError(err);

                        /* Key should be valid */
                        assert.strictEqual(typeof rawKey, 'string');
                        const parts = hdclient.keyscheme.deserialize(rawKey);
                        assert.strictEqual(parts.nDataParts, 3);
                        assert.strictEqual(parts.nCodingParts, 0);
                        assert.strictEqual(parts.nChunks, 1);

                        /* Check async mechanism */
                        const repTopic = hdmock.getTopic(hdClient, repairTopic);
                        const repLoggedErrors = [{
                            rawKey,
                            fragments: [[0, 2]],
                            version: 1,
                        }];
                        hdmock.strictCompareTopicContent(
                            repTopic, repLoggedErrors);
                        const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                        hdmock.strictCompareTopicContent(
                            delTopic, undefined);

                        done();
                    });
            });

            mocha.it('Double failure', function (done) {
                const codes = [{ type: 'CP', dataParts: 3, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes });
                const content = fs.createReadStream(
                    'tests/functional/random_payload');
                const mocks = [[
                    {
                        statusCode: 403,
                        payload: content,
                        contentType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                    },
                    {
                        statusCode: 500,
                        payload: content,
                        contentType: 'data',
                    },
                ]];

                hdmock.mockPUT(
                    hdClient, keyContext,
                    hdmock.getPayloadLength(content),
                    mocks);

                hdClient.put(
                    content,
                    hdmock.getPayloadLength(content),
                    keyContext, '1',
                    (err, rawKey) => {
                        /* Check for errors */
                        assert.ok(err);
                        assert.strictEqual(err.code, 500);

                        /* Key should still be valid */
                        assert.strictEqual(typeof rawKey, 'string');
                        const parts = hdclient.keyscheme.deserialize(rawKey);
                        assert.strictEqual(parts.nDataParts, 3);
                        assert.strictEqual(parts.nCodingParts, 0);
                        assert.strictEqual(parts.nChunks, 1);

                        /* Check cleanup mechanism */
                        const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                        const delLoggedErrors = [{
                            rawKey,
                            fragments: [[0, 0], [0, 1], [0, 2]],
                            version: 1,
                        }];
                        hdmock.strictCompareTopicContent(
                            delTopic, delLoggedErrors);
                        const repTopic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            repTopic, undefined);

                        done();
                    });
            });

            mocha.it('Timeout < 50%', function (done) {
                const codes = [{ type: 'CP', dataParts: 3, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes });
                const content = fs.createReadStream(
                    'tests/functional/random_payload');
                const mocks = [[
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                        timeoutMs: hdClient.options.requestTimeoutMs + 10,
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                    },
                ]];

                hdmock.mockPUT(
                    hdClient, keyContext,
                    hdmock.getPayloadLength(content),
                    mocks);

                hdClient.put(
                    content,
                    hdmock.getPayloadLength(content),
                    keyContext, '1',
                    (err, rawKey) => {
                        assert.ifError(err);
                        /* Key should still be valid */
                        assert.strictEqual(typeof rawKey, 'string');
                        const parts = hdclient.keyscheme.deserialize(rawKey);
                        assert.strictEqual(parts.nDataParts, 3);
                        assert.strictEqual(parts.nCodingParts, 0);
                        assert.strictEqual(parts.nChunks, 1);

                        /* Check cleanup mechanism */
                        const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                        hdmock.strictCompareTopicContent(
                            delTopic, undefined);
                        const repTopic = hdmock.getTopic(hdClient, repairTopic);
                        const chkLoggedErrors = [{
                            rawKey,
                            fragments: [[0, 1]],
                            version: 1,
                        }];
                        hdmock.strictCompareTopicContent(
                            repTopic, chkLoggedErrors);

                        /* Check for errors */
                        done(err);
                    });
            });

            mocha.it('Timeout >= 50%', function (done) {
                /**
                 * Should result in global failure and
                 * request all fragments to be cleaned up
                 */
                const codes = [{ type: 'CP', dataParts: 4, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 4, codes });
                const content = fs.createReadStream(
                    'tests/functional/random_payload');
                const mocks = [[
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                        timeoutMs: hdClient.options.requestTimeoutMs + 10,
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                        timeoutMs: hdClient.options.requestTimeoutMs + 10,
                    },
                ]];

                hdmock.mockPUT(
                    hdClient, keyContext,
                    hdmock.getPayloadLength(content),
                    mocks);

                hdClient.put(
                    content,
                    hdmock.getPayloadLength(content),
                    keyContext, '1',
                    (err, rawKey) => {
                        /* Check for errors */
                        assert.ok(err);
                        assert.strictEqual(err.code, 504);
                        assert.strictEqual(err.message, 'TimeoutError');

                        /* Key should still be valid */
                        assert.strictEqual(typeof rawKey, 'string');
                        const parts = hdclient.keyscheme.deserialize(rawKey);
                        assert.strictEqual(parts.nDataParts, 4);
                        assert.strictEqual(parts.nCodingParts, 0);
                        assert.strictEqual(parts.nChunks, 1);

                        /* Check cleanup mechanism */
                        const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                        const delLoggedErrors = [{
                            rawKey,
                            fragments: [[0, 0], [0, 1], [0, 2], [0, 3]],
                            version: 1,
                        }];
                        hdmock.strictCompareTopicContent(
                            delTopic, delLoggedErrors);
                        done();
                    });
            });
        });

        mocha.describe('Erasure coding', function () {
            mocha.it('All success - Compare with manual-XOR', function (done) {
                const code = 'RS';
                const k = 2;
                const codes = [{ type: code, dataParts: k, codingParts: 1, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: k + 1, codes });
                /* Test expects object size < stripeSize */
                const content = crypto.randomBytes(10);
                const { stripeSize } = hdclient.split.getSplitSize(
                    0, content.length, code, k);
                const dataPart1 = Buffer.alloc(stripeSize, 0);
                content.slice(0, stripeSize).copy(dataPart1);
                const dataPart2 = Buffer.alloc(stripeSize, 0);
                content.slice(stripeSize).copy(dataPart2);
                const codingPart = Buffer.allocUnsafe(stripeSize);
                for (let i = 0; i < stripeSize; ++i) {
                    codingPart[i] = dataPart1[i] ^ dataPart2[i];
                }

                const mocks = [[
                    {
                        statusCode: 200,
                        payload: dataPart1,
                        contentType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: dataPart2,
                        contentType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: codingPart,
                        contentType: 'data',
                    },
                ]];

                hdmock.mockPUT(
                    hdClient, keyContext,
                    hdmock.getPayloadLength(content),
                    mocks);

                hdClient.put(
                    hdmock.streamString(content),
                    hdmock.getPayloadLength(content),
                    keyContext, '1',
                    (err, rawKey) => {
                        assert.ifError(err);
                        /* Check generated key */
                        assert.strictEqual(typeof rawKey, 'string');
                        const parts = hdclient.keyscheme.deserialize(rawKey);
                        assert.strictEqual(parts.nDataParts, k);
                        assert.strictEqual(parts.nCodingParts, 1);
                        assert.strictEqual(parts.nChunks, 1);

                        /* Check cleanup mechanism */
                        const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                        hdmock.strictCompareTopicContent(
                            delTopic, undefined);
                        const repTopic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            repTopic, undefined);

                        /* Check for errors */
                        done(err);
                    });
            });

            [[2, 1], [4, 2], [5, 6]].forEach(args => {
                [64, 7777, 8192].forEach(size => {
                    const [k, m] = args;
                    const description = `k=${k}, m=${m}, size=${size}`;
                    const timeouts = new Map(hdclient.utils.range(m - 1).map(
                        () => [Math.floor(Math.random() * (k + m)), 1]));
                    mocha.it(`Success on harder code (${description})- timeouts=${timeouts.size}`, function (done) {
                        const code = 'RS';
                        const codes = [{ type: code, dataParts: k, codingParts: m, pattern: '.*' }];
                        const hdClient = hdmock.getDefaultClient({ nLocations: k + m, codes });
                        const content = crypto.randomBytes(size);
                        const { stripeSize } = hdclient.split.getSplitSize(
                            0, content.length, code, k);

                        new Promise(resolve => {
                            let todo = k + m;
                            const expectedBuffers = hdclient.utils.range(k + m).map(() => []);
                            const expectedStreams = hdclient.utils.range(k + m).map(i => {
                                const s = new stream.PassThrough();
                                s.on('data', c => expectedBuffers[i].push(c));
                                s.on('end', () => {
                                    todo--;
                                    if (todo === 0) {
                                        /* For some fucking reason map(Buffer.concat) doesn't work as expected */
                                        const flattened = expectedBuffers.map(
                                            bs => Buffer.concat(bs));
                                        resolve(flattened);
                                    }
                                });
                                return s;
                            });
                            ecstream.encode(
                                hdmock.streamString(content),
                                hdmock.getPayloadLength(content),
                                expectedStreams.slice(0, k),
                                expectedStreams.slice(k),
                                stripeSize);
                        }).then(buffers => {
                            const mocks = [buffers.map((b, i) => ({
                                statusCode: 200,
                                payload: b,
                                contentType: 'data',
                                timeoutMs: timeouts.has(i) ?
                                    hdClient.options.requestTimeoutMs + 10 : 0,
                            }))];

                            hdmock.mockPUT(
                                hdClient, keyContext,
                                size,
                                mocks);

                            hdClient.put(
                                hdmock.streamString(content),
                                hdmock.getPayloadLength(content),
                                keyContext, '1',
                                (err, rawKey) => {
                                    assert.ifError(err);
                                    /* Check generated key */
                                    assert.strictEqual(typeof rawKey, 'string');
                                    const parts = hdclient.keyscheme.deserialize(rawKey);
                                    assert.strictEqual(parts.nDataParts, k);
                                    assert.strictEqual(parts.nCodingParts, m);
                                    assert.strictEqual(parts.nChunks, 1);

                                    /* Check cleanup mechanism */
                                    const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                                    hdmock.strictCompareTopicContent(
                                        delTopic, undefined);
                                    const repTopic = hdmock.getTopic(hdClient, repairTopic);
                                    let expectedChkTopic = undefined;
                                    if (timeouts.size > 0) {
                                        expectedChkTopic = [{
                                            rawKey,
                                            fragments: [...timeouts.keys()].sort((a, b) => (a - b))
                                                .map(k => [0, k]),
                                            version: 1,
                                        }];
                                    }
                                    hdmock.strictCompareTopicContent(
                                        repTopic, expectedChkTopic);

                                    /* Check for errors */
                                    done(err);
                                });
                        }).catch(err => done(err));
                    });
                });
            });
        });

        mocha.describe('Code selection', function () {
            mocha.it('Success', function (done) {
                const codes = [
                    { type: 'CP', dataParts: 1, codingParts: 0, pattern: 'superspecific/bestObjEver' },
                    { type: 'CP', dataParts: 3, codingParts: 0, pattern: 'testbuc.*/best.*' },
                    { type: 'RS', dataParts: 2, codingParts: 1, pattern: '.*' },
                ];
                const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes });
                const content = 'Je suis une mite en pullover';
                const mocks = [[
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                    },
                ]];

                hdmock.mockPUT(
                    hdClient, keyContext,
                    hdmock.getPayloadLength(content),
                    mocks);

                hdClient.put(
                    hdmock.streamString(content),
                    hdmock.getPayloadLength(content),
                    keyContext, '1',
                    err => done(err));
            });

            mocha.it('No match found', function (done) {
                const codes = [{ type: 'CP', dataParts: 1, codingParts: 0, pattern: 'superspecific/bestObjEver' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 1, codes });
                const content = 'Je suis une mite en pullover';
                const mocks = [[
                    {
                        statusCode: 200,
                        payload: content,
                        contentType: 'data',
                    },
                ]];

                hdmock.mockPUT(
                    hdClient, keyContext,
                    hdmock.getPayloadLength(content),
                    mocks);

                hdClient.put(
                    hdmock.streamString(content),
                    hdmock.getPayloadLength(content),
                    keyContext, '1',
                    err => {
                        assert.ok(err);
                        assert.strictEqual(err.message, 'ConfigError');
                        assert.strictEqual(err.code, 422);
                        assert.strictEqual(err.description, 'No matching code pattern found');
                        done();
                    });
            });
        });
    });

    mocha.describe('Persisting error edge cases', function () {
        mocha.it('Failed to persist', function (done) {
            /* Same exact scenario as 'Timeout < 50%'
             * but we failed to persist errors/warnings,
             * expecting resulting error
             */
            const codes = [{ type: 'CP', dataParts: 3, codingParts: 0, pattern: '.*' }];
            const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes });
            const content = fs.createReadStream(
                'tests/functional/random_payload');
            const mocks = [[
                {
                    statusCode: 200,
                    payload: content,
                    contentType: 'data',
                },
                {
                    statusCode: 200,
                    payload: content,
                    contentType: 'data',
                    timeoutMs: hdClient.options.requestTimeoutMs + 10,
                },
                {
                    statusCode: 200,
                    payload: content,
                    contentType: 'data',
                },
            ]];

            hdmock.mockPUT(
                hdClient, keyContext,
                hdmock.getPayloadLength(content),
                mocks);
            hdClient.errorAgent.nextError = new Error('Broken by Design');

            hdClient.put(
                content,
                hdmock.getPayloadLength(content),
                keyContext, '1',
                (err, rawKey) => {
                    /* Check for errors */
                    assert.ok(err);
                    assert.strictEqual(err.code, 500);
                    assert.strictEqual(err.message, 'InternalError');
                    assert.strictEqual(
                        err.description,
                        'Failed to persist bad fragments: Broken by Design'
                    );
                    /* Key should still be valid */
                    assert.strictEqual(typeof rawKey, 'string');
                    const parts = hdclient.keyscheme.deserialize(rawKey);
                    assert.strictEqual(parts.nDataParts, 3);
                    assert.strictEqual(parts.nCodingParts, 0);
                    assert.strictEqual(parts.nChunks, 1);

                    /* Check cleanup mechanism */
                    const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        delTopic, undefined);
                    const repTopic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        repTopic, undefined);
                    done();
                });
        });
    });

    mocha.describe('Input stream  failures', function () {
        mocha.it('Error', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const content = fs.createReadStream(
                'tests/functional/random_payload');

            /* Fake error when half-way through */
            const leftover = Math.floor(hdmock.getPayloadLength(content) / 2);
            const errorStream = new stream.Transform({
                transform(chunk, encoding, callback) {
                    if (leftover < chunk.length) {
                        this.emit('error', new Error('My bad...'));
                    } else {
                        this.leftover -= chunk.length;
                        this.push(chunk);
                    }
                    callback();
                },
            });

            const mocks = [[
                {
                    statusCode: 200,
                    payload: content,
                    contentType: 'data',
                },
            ]];

            hdmock.mockPUT(
                hdClient, keyContext,
                hdmock.getPayloadLength(content),
                mocks);
            content.pipe(errorStream);

            hdClient.put(
                errorStream,
                hdmock.getPayloadLength(content),
                keyContext, '1',
                (err, rawKey) => {
                    /* Check generated key */
                    assert.strictEqual(typeof rawKey, 'string');
                    hdclient.keyscheme.deserialize(rawKey);

                    /* Check cleanup mechanism */
                    const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        delTopic,
                        [{
                            rawKey,
                            fragments: [[0, 0]],
                            version: 1,
                        }]);
                    const repTopic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        repTopic, undefined);

                    /* Check for errors */
                    assert.ok(err);
                    assert.strictEqual(err.code, 500);
                    assert.strictEqual(err.message, 'PUTError');
                    assert.strictEqual(err.description, 'My bad...');
                    done();
                });
        });
    });

    mocha.describe('Split', function () {
        mocha.it('Success', function (done) {
            const content = crypto.randomBytes(30000);
            const size = hdmock.getPayloadLength(content);
            const minSplitSize = size / 3;
            const realSplitSize = hdclient.split.align(
                minSplitSize, hdclient.split.DATA_ALIGN);
            assert.ok(size > realSplitSize);
            const codes = [{ type: 'CP', dataParts: 2, codingParts: 0, pattern: '.*' }];
            const hdClient = hdmock.getDefaultClient({ nLocations: 2, codes, minSplitSize });
            const mocks = [
                [{
                    statusCode: 200,
                    payload: content.slice(0, realSplitSize),
                    contentType: 'data',
                }, {
                    statusCode: 200,
                    payload: content.slice(0, realSplitSize),
                    contentType: 'data',
                }],
                [{
                    statusCode: 200,
                    payload: content.slice(realSplitSize, 2 * realSplitSize),
                    contentType: 'data',
                }, {
                    statusCode: 200,
                    payload: content.slice(realSplitSize, 2 * realSplitSize),
                    contentType: 'data',
                }],
                [{
                    statusCode: 200,
                    payload: content.slice(2 * realSplitSize),
                    contentType: 'data',
                }, {
                    statusCode: 200,
                    payload: content.slice(2 * realSplitSize),
                    contentType: 'data',
                }],
            ];

            hdmock.mockPUT(hdClient, keyContext, size, mocks);

            hdClient.put(
                hdmock.streamString(content),
                size,
                keyContext, '1',
                (err, rawKey) => {
                    assert.ifError(err);
                    /* Check generated key */
                    assert.strictEqual(typeof rawKey, 'string');
                    const parts = hdclient.keyscheme.deserialize(rawKey);

                    /* Verify split part */
                    assert.strictEqual(parts.nDataParts, 2);
                    assert.strictEqual(parts.nCodingParts, 0);
                    assert.strictEqual(parts.nChunks, 3);
                    assert.strictEqual(parts.size, size);
                    const expectedSplitSize = hdclient.split.align(
                        minSplitSize, hdclient.split.DATA_ALIGN);
                    assert.strictEqual(parts.splitSize, expectedSplitSize);

                    /* Verify layout: fragment (i,j) sould be on hyperdrive i for all j */
                    for (let i = 0; i < 2; ++i) {
                        const uuid = hdClient.conf.policy.cluster.components[i].name;
                        for (let j = 0; j < 3; ++j) {
                            const fragment = parts.chunks[j].data[i];
                            assert.strictEqual(fragment.uuid, uuid);
                            assert.ok(fragment.key, keyContext.objectKey);
                        }
                    }

                    /* Check cleanup mechanism */
                    const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        delTopic, undefined);
                    const repTopic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        repTopic, undefined);

                    /* Check for errors */
                    done(err);
                });
        });

        mocha.it('Sprinkled errors', function (done) {
            const content = crypto.randomBytes(30000);
            const size = hdmock.getPayloadLength(content);
            const minSplitSize = size / 3;
            const realSplitSize = hdclient.split.align(
                minSplitSize, hdclient.split.DATA_ALIGN);
            assert.ok(size > realSplitSize);
            const codes = [{ type: 'CP', dataParts: 3, codingParts: 0, pattern: '.*' }];
            const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes, minSplitSize });
            const mocks = [
                [{
                    statusCode: 200,
                    payload: content.slice(0, realSplitSize),
                    contentType: 'data',
                    timeoutMs: hdClient.options.requestTimeoutMs + 10,
                }, {
                    statusCode: 200,
                    payload: content.slice(0, realSplitSize),
                    contentType: 'data',
                }, {
                    statusCode: 200,
                    payload: content.slice(0, realSplitSize),
                    contentType: 'data',
                }],
                [{
                    statusCode: 200,
                    payload: content.slice(realSplitSize, 2 * realSplitSize),
                    contentType: 'data',
                }, {
                    statusCode: 200,
                    payload: content.slice(realSplitSize, 2 * realSplitSize),
                    contentType: 'data',
                }, {
                    statusCode: 500,
                    payload: content.slice(0, realSplitSize),
                    contentType: 'data',
                }],
                [{
                    statusCode: 403,
                    payload: content.slice(2 * realSplitSize),
                    contentType: 'data',
                }, {
                    statusCode: 200,
                    payload: content.slice(2 * realSplitSize),
                    contentType: 'data',
                }, {
                    statusCode: 200,
                    payload: content.slice(0, realSplitSize),
                    contentType: 'data',
                }],
            ];

            hdmock.mockPUT(hdClient, keyContext, size, mocks);

            hdClient.put(
                hdmock.streamString(content),
                size,
                keyContext, '1',
                (err, rawKey) => {
                    assert.ok(err);
                    assert.strictEqual(err.code, 500);

                    /* Check cleanup mechanism - everything to be deleted (safe side) */
                    const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        delTopic,
                        [{
                            rawKey,
                            fragments: [[0, 0], [0, 1], [0, 2],
                                        [1, 0], [1, 1], [1, 2],
                                        [2, 0], [2, 1], [2, 2]],
                            version: 1,

                        }]);

                    const repTopic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        repTopic, undefined);

                    /* Check for errors */
                    done();
                });
        });
    });
});
