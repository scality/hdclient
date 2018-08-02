'use strict'; // eslint-disable-line strict
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');
const nock = require('nock');
const fs = require('fs');
const stream = require('stream');

const hdclient = require('../../index');
const hdmock = require('../utils');

mocha.describe('PUT', function () {
    // Clean all HTTP mocks before starting the test
    mocha.beforeEach(nock.cleanAll);

    // Verify all mocks have been used - no garbage
    mocha.afterEach(function () { assert.ok(nock.isDone); });

    const deleteTopic = hdclient.httpUtils.topics.delete;
    const checkTopic = hdclient.httpUtils.topics.check;

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
                    const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        delTopic, undefined);
                    const chkTopic = hdmock.getTopic(hdClient, checkTopic);
                    hdmock.strictCompareTopicContent(
                        chkTopic, undefined);

                    /* Check for errors */
                    done(err);
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
                    const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        delTopic, undefined);
                    const chkTopic = hdmock.getTopic(hdClient, checkTopic);
                    hdmock.strictCompareTopicContent(
                        chkTopic, undefined);

                    /* Check for errors */
                    done(err);
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
            const keyContext = {
                objectKey: 'bestObjEver',
            };

            hdmock.mockPUT(hdClient.options, keyContext, mocks);

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
                        }]);
                    const chkTopic = hdmock.getTopic(hdClient, checkTopic);
                    hdmock.strictCompareTopicContent(
                        chkTopic, undefined);

                    /* Check for errors */
                    assert.strictEqual(err.infos.status,
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
            const keyContext = {
                objectKey: 'bestObjEver',
            };

            hdmock.mockPUT(hdClient.options, keyContext, mocks);

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
                    }];
                    hdmock.strictCompareTopicContent(
                        delTopic, delLoggedErrors);
                    const chkTopic = hdmock.getTopic(hdClient, checkTopic);
                    hdmock.strictCompareTopicContent(
                        chkTopic, undefined);

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
                        const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                        hdmock.strictCompareTopicContent(
                            delTopic, undefined);
                        const chkTopic = hdmock.getTopic(hdClient, checkTopic);
                        hdmock.strictCompareTopicContent(
                            chkTopic, undefined);

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
                        const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                        const delLoggedErrors = [{
                            rawKey,
                            fragments: [[0, 0], [0, 1], [0, 2]],
                        }];
                        hdmock.strictCompareTopicContent(
                            delTopic, delLoggedErrors);
                        const chkTopic = hdmock.getTopic(hdClient, checkTopic);
                        hdmock.strictCompareTopicContent(
                            chkTopic, undefined);

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
                        const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                        const delLoggedErrors = [{
                            rawKey,
                            fragments: [[0, 0], [0, 1], [0, 2]],
                        }];
                        hdmock.strictCompareTopicContent(
                            delTopic, delLoggedErrors);
                        const chkTopic = hdmock.getTopic(hdClient, checkTopic);
                        hdmock.strictCompareTopicContent(
                            chkTopic, undefined);

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
                        const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                        hdmock.strictCompareTopicContent(
                            delTopic, undefined);
                        const chkTopic = hdmock.getTopic(hdClient, checkTopic);
                        const chkLoggedErrors = [{
                            rawKey,
                            fragments: [[0, 1]],
                        }];
                        hdmock.strictCompareTopicContent(
                            chkTopic, chkLoggedErrors);

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
                        const delTopic = hdmock.getTopic(hdClient, deleteTopic);
                        const delLoggedErrors = [{
                            rawKey,
                            fragments: [[0, 0], [0, 1], [0, 2], [0, 3]],
                        }];
                        hdmock.strictCompareTopicContent(
                            delTopic, delLoggedErrors);
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
            const hdClient = hdmock.getDefaultClient({
                nLocations: 3,
                code: 'CP',
                nData: 3,
                nCoding: 0,
            });
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
            const keyContext = {
                objectKey: 'bestObjEver',
            };

            hdmock.mockPUT(hdClient.options, keyContext, mocks);
            hdClient.errorAgent.nextError = new Error('Failed to queue');

            hdClient.put(
                content,
                hdmock.getPayloadLength(content),
                keyContext, '1',
                (err, rawKey) => {
                    /* Check for errors */
                    assert.ok(err);
                    assert.strictEqual(err.infos.status, 500);
                    assert.strictEqual(err.message, 'Failed to queue');

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
                    const chkTopic = hdmock.getTopic(hdClient, checkTopic);
                    hdmock.strictCompareTopicContent(
                        chkTopic, undefined);
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
            const keyContext = {
                objectKey: 'bestObjEver',
            };

            hdmock.mockPUT(hdClient.options, keyContext, mocks);
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
                        }]);
                    const chkTopic = hdmock.getTopic(hdClient, checkTopic);
                    hdmock.strictCompareTopicContent(
                        chkTopic, undefined);

                    /* Check for errors */
                    assert.ok(err);
                    assert.strictEqual(err.message, 'My bad...');
                    done();
                });
        });
    });
});
