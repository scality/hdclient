'use strict'; // eslint-disable-line strict
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');
const nock = require('nock');
const fs = require('fs');

const hdclient = require('../../index');
const hdmock = require('../utils');

mocha.describe('PUT', function () {
    // Clean all HTTP mocks before starting the test
    mocha.beforeEach(nock.cleanAll);

    // Verify all mocks have been used - no garbage
    mocha.afterEach(function () { assert.ok(nock.isDone); });

    const deleteTopic = hdclient.httpUtils.topics.delete;

    mocha.describe('Single hyperdrive', function () {
        mocha.it('Success small key', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const mocks = [
                {
                    statusCode: 200,
                    payload: 'Je suis une mite en pullover',
                    contentType: 'data',
                },
            ];
            const keyContext = {
                objectKey: 'bestObjEver',
            };

            hdmock.mockPUT(hdClient.options, keyContext, mocks);

            hdClient.put(
                hdmock.streamString(mocks[0].payload),
                hdmock.getPayloadLength(mocks[0].payload),
                keyContext, '1',
                (err, rawKey) => {
                    /* Check generated key */
                    assert.strictEqual(typeof rawKey, 'string');
                    const parts = hdclient.keyscheme.deserialize(rawKey);

                    const [endpoint, port] =
                              hdClient.options.policy.locations[0].split(':');
                    assert.strictEqual(parts.nDataParts, 1);
                    assert.strictEqual(parts.nCodingParts, 0);
                    assert.strictEqual(parts.nChunks, 1);
                    const fragment = parts.chunks[0].data[0];
                    assert.strictEqual(fragment.hostname, endpoint);
                    assert.strictEqual(fragment.port, Number(port));
                    assert.ok(fragment.key, keyContext.objectKey);

                    /* Check cleanup mechanism */
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareDeleteTopicContent(
                        topic, undefined);

                    /* Check for errors */
                    done(err);
                });
        });

        mocha.it('Success larger key (32 KiB)', function (done) {
            const hdClient = hdmock.getDefaultClient();
            /* TODO avoid depending on hardcoded path */
            const content = fs.createReadStream(
                'tests/functional/random_payload');
            const mocks = [
                {
                    statusCode: 200,
                    payload: content,
                    contentType: 'data',
                },
            ];
            const keyContext = {
                objectKey: 'bestObjEver',
            };

            hdmock.mockPUT(hdClient.options, keyContext, mocks);

            hdClient.put(
                content,
                hdmock.getPayloadLength(content),
                keyContext, '1',
                (err, rawKey) => {
                    /* Check generated key */
                    assert.strictEqual(typeof rawKey, 'string');
                    const parts = hdclient.keyscheme.deserialize(rawKey);

                    const [endpoint, port] =
                              hdClient.options.policy.locations[0].split(':');
                    assert.strictEqual(parts.nDataParts, 1);
                    assert.strictEqual(parts.nCodingParts, 0);
                    assert.strictEqual(parts.nChunks, 1);
                    const fragment = parts.chunks[0].data[0];
                    assert.strictEqual(fragment.hostname, endpoint);
                    assert.strictEqual(fragment.port, Number(port));
                    assert.ok(fragment.key, keyContext.objectKey);

                    /* Check cleanup mechanism */
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareDeleteTopicContent(
                        topic, undefined);

                    /* Check for errors */
                    done(err);
                });
        });

        mocha.it('Server error', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const mocks = [
                {
                    statusCode: 500,
                    payload: 'Je suis une mite en pullover',
                    contentType: 'data',
                },
            ];
            const keyContext = {
                objectKey: 'bestObjEver',
            };

            hdmock.mockPUT(hdClient.options, keyContext, mocks);

            hdClient.put(
                hdmock.streamString(mocks[0].payload),
                hdmock.getPayloadLength(mocks[0].payload),
                keyContext, '1',
                err => {
                    /* Check cleanup mechanism */
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareDeleteTopicContent(
                        topic, undefined);

                    /* Check for errors */
                    assert.strictEqual(err.infos.status, mocks[0].statusCode);
                    done();
                });
        });

        mocha.it('Timeout', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const mocks = [
                {
                    statusCode: 200,
                    payload: 'Je suis une mite en pullover',
                    contentType: 'data',
                    timeoutMs: hdClient.options.requestTimeoutMs + 10,
                },
            ];
            const keyContext = {
                objectKey: 'bestObjEver',
            };

            hdmock.mockPUT(hdClient.options, keyContext, mocks);

            let called = false;
            hdClient.put(
                hdmock.streamString(mocks[0].payload),
                hdmock.getPayloadLength(mocks[0].payload),
                keyContext, '1',
                (err, rawKey) => {
                    assert.ok(!called);
                    called = true;

                    /* Check cleanup mechanism */
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    const expectedLoggedErrors = [{
                        rawKey,
                        toDelete: [[0, 0]],
                    }];
                    hdmock.strictCompareDeleteTopicContent(
                        topic, expectedLoggedErrors);

                    /* Check for errors */
                    assert.strictEqual(err.infos.status, 500);
                    done();
                });
        });
    });

    mocha.describe('Multiple hyperdrives', function () {
        mocha.describe('Replication', function () {
            mocha.it('All Success', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 3,
                    code: 'CP',
                    nData: 3,
                    nCoding: 0,
                });
                const content = 'Je suis une mite en pullover';
                const mocks = [
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
                ];
                const keyContext = {
                    objectKey: 'bestObjEver',
                };

                hdmock.mockPUT(hdClient.options, keyContext, mocks);

                hdClient.put(
                    hdmock.streamString(content),
                    hdmock.getPayloadLength(content),
                    keyContext, '1',
                    (err, rawKey) => {
                        /* Check generated key */
                        assert.strictEqual(typeof rawKey, 'string');
                        const parts = hdclient.keyscheme.deserialize(rawKey);
                        assert.strictEqual(parts.nDataParts, 3);
                        assert.strictEqual(parts.nCodingParts, 0);
                        assert.strictEqual(parts.nChunks, 1);

                        /* Check cleanup mechanism */
                        const topic = hdmock.getTopic(hdClient, deleteTopic);
                        hdmock.strictCompareDeleteTopicContent(
                            topic, undefined);

                        /* Check for errors */
                        done(err);
                    });
            });

            mocha.it('Single failure', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 3,
                    code: 'CP',
                    nData: 3,
                    nCoding: 0,
                });
                const content = fs.createReadStream(
                    'tests/functional/random_payload');
                const mocks = [
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
                ];
                const keyContext = {
                    objectKey: 'bestObjEver',
                };

                hdmock.mockPUT(hdClient.options, keyContext, mocks);

                hdClient.put(
                    content,
                    hdmock.getPayloadLength(content),
                    keyContext, '1',
                    (err, rawKey) => {
                        /* Check for errors */
                        assert.ok(err);
                        assert.strictEqual(err.infos.status, 500);

                        /* Key should still be valid */
                        assert.strictEqual(typeof rawKey, 'string');
                        const parts = hdclient.keyscheme.deserialize(rawKey);
                        assert.strictEqual(parts.nDataParts, 3);
                        assert.strictEqual(parts.nCodingParts, 0);
                        assert.strictEqual(parts.nChunks, 1);

                        /* Check cleanup mechanism */
                        const topic = hdmock.getTopic(hdClient, deleteTopic);
                        const expectedLoggedErrors = [{
                            rawKey,
                            toDelete: [[0, 0], [0, 1]],
                        }];
                        hdmock.strictCompareDeleteTopicContent(
                            topic, expectedLoggedErrors);
                        done();
                    });
            });

            mocha.it('Double failure', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 3,
                    code: 'CP',
                    nData: 3,
                    nCoding: 0,
                });
                const content = fs.createReadStream(
                    'tests/functional/random_payload');
                const mocks = [
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
                ];
                const keyContext = {
                    objectKey: 'bestObjEver',
                };

                hdmock.mockPUT(hdClient.options, keyContext, mocks);

                hdClient.put(
                    content,
                    hdmock.getPayloadLength(content),
                    keyContext, '1',
                    (err, rawKey) => {
                        /* Check for errors */
                        assert.ok(err);
                        assert.strictEqual(err.infos.status, 500);

                        /* Key should still be valid */
                        assert.strictEqual(typeof rawKey, 'string');
                        const parts = hdclient.keyscheme.deserialize(rawKey);
                        assert.strictEqual(parts.nDataParts, 3);
                        assert.strictEqual(parts.nCodingParts, 0);
                        assert.strictEqual(parts.nChunks, 1);

                        /* Check cleanup mechanism */
                        const topic = hdmock.getTopic(hdClient, deleteTopic);
                        const expectedLoggedErrors = [{
                            rawKey,
                            toDelete: [[0, 1]],
                        }];
                        hdmock.strictCompareDeleteTopicContent(
                            topic, expectedLoggedErrors);
                        done();
                    });
            });

            mocha.it('Timeout < 50%', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 3,
                    code: 'CP',
                    nData: 3,
                    nCoding: 0,
                });
                const content = fs.createReadStream(
                    'tests/functional/random_payload');
                const mocks = [
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
                ];
                const keyContext = {
                    objectKey: 'bestObjEver',
                };

                hdmock.mockPUT(hdClient.options, keyContext, mocks);

                hdClient.put(
                    content,
                    hdmock.getPayloadLength(content),
                    keyContext, '1',
                    (err, rawKey) => {
                        /* Key should still be valid */
                        assert.strictEqual(typeof rawKey, 'string');
                        const parts = hdclient.keyscheme.deserialize(rawKey);
                        assert.strictEqual(parts.nDataParts, 3);
                        assert.strictEqual(parts.nCodingParts, 0);
                        assert.strictEqual(parts.nChunks, 1);

                        /* Check cleanup mechanism */
                        const topic = hdmock.getTopic(hdClient, deleteTopic);
                        hdmock.strictCompareDeleteTopicContent(
                            topic, undefined);

                        /* Check for errors */
                        done(err);
                    });
            });

            mocha.it('Timeout >= 50%', function (done) {
                /**
                 * Should result in global failure and
                 * request all fragments to be cleaned up
                 */
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 4,
                    code: 'CP',
                    nData: 4,
                    nCoding: 0,
                });
                const content = fs.createReadStream(
                    'tests/functional/random_payload');
                const mocks = [
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
                ];
                const keyContext = {
                    objectKey: 'bestObjEver',
                };

                hdmock.mockPUT(hdClient.options, keyContext, mocks);

                hdClient.put(
                    content,
                    hdmock.getPayloadLength(content),
                    keyContext, '1',
                    (err, rawKey) => {
                        /* Check for errors */
                        assert.ok(err);
                        assert.strictEqual(err.infos.status, 500);

                        /* Key should still be valid */
                        assert.strictEqual(typeof rawKey, 'string');
                        const parts = hdclient.keyscheme.deserialize(rawKey);
                        assert.strictEqual(parts.nDataParts, 4);
                        assert.strictEqual(parts.nCodingParts, 0);
                        assert.strictEqual(parts.nChunks, 1);

                        /* Check cleanup mechanism */
                        const topic = hdmock.getTopic(hdClient, deleteTopic);
                        const expectedLoggedErrors = [{
                            rawKey,
                            toDelete: [[0, 0], [0, 1], [0, 2], [0, 3]],
                        }];
                        hdmock.strictCompareDeleteTopicContent(
                            topic, expectedLoggedErrors);
                        done();
                    });
            });
        });
    });
});
