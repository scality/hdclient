'use strict'; // eslint-disable-line strict
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');
const nock = require('nock');
const fs = require('fs');
const crypto = require('crypto');

const hdclient = require('../../index');
const hdmock = require('../utils');

const BadKeyError = hdclient.keyscheme.KeySchemeDeserializeError;

mocha.describe('Hyperdrive Client GET', function () {
    // Clean all HTTP mocks before starting the test
    mocha.beforeEach(nock.cleanAll);

    // Verify all mocks have been used - no garbage
    mocha.afterEach(function () { assert.ok(nock.isDone); });

    const repairTopic = hdclient.httpUtils.topics.repair;

    mocha.describe('Single hyperdrive', function () {
        mocha.it('Existing small key', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const content = 'Je suis une mite en pullover';
            const mockOptions = {
                statusCode: 200,
                payload: content,
                acceptType: 'data',
            };
            const [rawKey] = hdmock.mockGET(
                hdClient.options, 'bestObjEver', [mockOptions]
            );

            hdClient.get(
                rawKey, undefined /* range */, '1',
                (err, httpReply) => {
                    assert.ok(httpReply);

                    // Sanity checks before buffering the stream
                    assert.strictEqual(httpReply.statusCode,
                                       mockOptions.statusCode);
                    assert.strictEqual(httpReply.headers['content-length'],
                                       content.length);

                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);

                    // Buffer the whole stream and perform checks on 'end' event
                    const readBufs = [];
                    httpReply.on('data', function (chunk) {
                        readBufs.push(chunk);
                    });
                    httpReply.on('end', function () {
                        const buf = readBufs.join('');
                        assert.strictEqual(buf.length, content.length);
                        assert.strictEqual(buf, content);
                        done(err);
                    });
                });
        });

        mocha.it('Existing larger key (32 KiB)', function (done) {
            const hdClient = hdmock.getDefaultClient();
            /* TODO avoid depending on hardcoded path */
            const content = fs.createReadStream(
                'tests/functional/random_payload');
            const expectedDigest = '2b7a12623e736ee1773fc3efc6c289e8';
            const contentLength = hdmock.getPayloadLength(content);
            const mockOptions = {
                statusCode: 200,
                payload: content,
                acceptType: 'data',
            };
            const [rawKey] = hdmock.mockGET(
                hdClient.options, 'bestObjEver', [mockOptions]
            );

            hdClient.get(
                rawKey, undefined /* range */, '1',
                (err, httpReply) => {
                    assert.ok(httpReply);

                    // Sanity checks before buffering the stream
                    assert.strictEqual(httpReply.statusCode,
                                       mockOptions.statusCode);
                    assert.strictEqual(httpReply.headers['content-length'],
                                       contentLength);

                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);

                    // Compute return valud md5
                    const hash = crypto.createHash('md5');
                    httpReply.on('data', function (data) {
                        hash.update(data, 'utf8');
                    });
                    httpReply.on('end', function () {
                        const getDigest = hash.digest('hex');
                        assert.strictEqual(getDigest, expectedDigest);
                        done(err);
                    });
                });
        });

        mocha.it('Half range', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const content = 'Je suis une mite en pullover';
            const range = [4];
            const mockOptions = {
                statusCode: 200,
                payload: content,
                acceptType: 'data',
                range,
            };
            const expectedContent = content.slice(...range);
            const [rawKey] = hdmock.mockGET(
                hdClient.options, 'bestObjEver', [mockOptions]
            );

            hdClient.get(
                rawKey, range, '1',
                (err, httpReply) => {
                    assert.ok(httpReply);

                    // Sanity checks before buffering the stream
                    assert.strictEqual(httpReply.statusCode,
                                       mockOptions.statusCode);
                    assert.strictEqual(httpReply.headers['content-length'],
                                       expectedContent.length);

                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);

                    // Buffer the whole stream and perform checks on 'end' event
                    const readBufs = [];
                    httpReply.on('data', function (chunk) {
                        readBufs.push(chunk);
                    });
                    httpReply.on('end', function () {
                        const buf = readBufs.join('');
                        assert.strictEqual(buf.length, expectedContent.length);
                        assert.strictEqual(buf, expectedContent);
                        done(err);
                    });
                });
        });

        mocha.it('Full range', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const range = [4, 8];
            const content = 'Je suis une mite en pullover';
            // Slice right end is not inclusive but HTTP ranges are
            const expectedContent = content.slice(range[0], range[1] + 1);
            const mockOptions = {
                statusCode: 200,
                payload: content,
                acceptType: 'data',
                range,
            };
            const [rawKey] = hdmock.mockGET(
                hdClient.options, 'bestObjEver', [mockOptions]
            );

            hdClient.get(
                rawKey, range, '1',
                (err, httpReply) => {
                    assert.ok(httpReply);

                    // Sanity checks before buffering the stream
                    assert.strictEqual(httpReply.statusCode,
                                       mockOptions.statusCode);
                    assert.strictEqual(httpReply.headers['content-length'],
                                       expectedContent.length);

                    // Buffer the whole stream and perform checks on 'end' event
                    const readBufs = [];
                    httpReply.on('data', function (chunk) {
                        readBufs.push(chunk);
                    });
                    httpReply.on('end', function () {
                        const buf = readBufs.join('');
                        assert.strictEqual(buf.length, expectedContent.length);
                        assert.strictEqual(buf, expectedContent);
                        done(err);
                    });
                });
        });

        mocha.it('Larger-than-size range', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const range = [4, 9999999];
            const content = 'Je suis une mite en pullover';
            const expectedContent = content.slice(...range);
            const mockOptions = {
                statusCode: 200,
                payload: content,
                acceptType: 'data',
                range,
            };
            const [rawKey] = hdmock.mockGET(
                hdClient.options, 'bestObjEver', [mockOptions]
            );

            hdClient.get(
                rawKey, range, '1',
                (err, httpReply) => {
                    assert.ok(httpReply);

                    // Sanity checks before buffering the stream
                    assert.strictEqual(httpReply.statusCode,
                                       mockOptions.statusCode);
                    assert.strictEqual(httpReply.headers['content-length'],
                                       expectedContent.length);

                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);

                    // Buffer the whole stream and perform checks on 'end' event
                    const readBufs = [];
                    httpReply.on('data', function (chunk) {
                        readBufs.push(chunk);
                    });
                    httpReply.on('end', function () {
                        const buf = readBufs.join('');
                        assert.strictEqual(buf.length, expectedContent.length);
                        assert.strictEqual(buf, expectedContent);
                        done(err);
                    });
                });
        });

        mocha.it('First byte only', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const range = [0, 0];
            const content = 'Je suis une mite en pullover';
            const expectedContent = content[0];
            const mockOptions = {
                statusCode: 200,
                payload: content,
                acceptType: 'data',
                range,
            };
            const [rawKey] = hdmock.mockGET(
                hdClient.options, 'bestObjEver', [mockOptions]
            );

            hdClient.get(
                rawKey, range, '1',
                (err, httpReply) => {
                    assert.ok(httpReply);

                    // Sanity checks before buffering the stream
                    assert.strictEqual(httpReply.statusCode,
                                       mockOptions.statusCode);
                    assert.strictEqual(httpReply.headers['content-length'],
                                       expectedContent.length);

                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);

                    // Buffer the whole stream and perform checks on 'end' event
                    const readBufs = [];
                    httpReply.on('data', function (chunk) {
                        readBufs.push(chunk);
                    });
                    httpReply.on('end', function () {
                        const buf = readBufs.join('');
                        assert.strictEqual(buf.length, expectedContent.length);
                        assert.strictEqual(buf, expectedContent);
                        done(err);
                    });
                });
        });

        mocha.it('Not found key', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const [rawKey] = hdmock.mockGET(
                hdClient.options,
                'bestObjEver',
                [{
                    statusCode: 404,
                    payload: '',
                    acceptType: 'data',
                }]
            );

            hdClient.get(rawKey, null /* range */, '1', err => {
                assert.strictEqual(err.infos.status, 404);

                const topic = hdmock.getTopic(hdClient, repairTopic);
                hdmock.strictCompareTopicContent(
                    topic, [{
                        rawKey,
                        fragments: [[0, 0]],
                    }]);
                done();
            });
        });

        mocha.it('Server error', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const [rawKey] = hdmock.mockGET(
                hdClient.options,
                'bestObjEver',
                [{
                    statusCode: 500,
                    payload: '',
                    acceptType: 'data',
                }]
            );

            hdClient.get(rawKey, null /* range */, '1', err => {
                assert.strictEqual(err.infos.status, 500);
                const topic = hdmock.getTopic(hdClient, repairTopic);
                hdmock.strictCompareTopicContent(
                        topic, undefined);
                done();
            });
        });

        mocha.it('Bad key', function (done) {
            const hdClient = hdmock.getDefaultClient();
            hdClient.delete('---', '1', err => {
                if (!(err instanceof BadKeyError)) {
                    throw err;
                }
                done();
            });
        });

        mocha.it('Timeout', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const mockDelay = hdClient.options.requestTimeoutMs + 10;
            const [rawKey] = hdmock.mockGET(
                hdClient.options,
                'bestObjEver',
                [{
                    statusCode: 200,
                    payload: 'gné',
                    acceptType: 'data',
                    timeoutMs: mockDelay,
                }]
            );

            hdClient.get(
                rawKey, undefined /* range */, '1',
                err => {
                    assert.ok(err);
                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);
                    done();
                });
        });
    });

    mocha.describe('Multiple hyperdrives', function () {
        mocha.describe('Replication', function () {
            mocha.it('All success', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 2,
                    code: 'CP',
                    nData: 2,
                });
                const content = 'Je suis une mite en pullover';
                const mockOptions = [
                    {
                        statusCode: 200,
                        payload: content,
                        acceptType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        acceptType: 'data',
                    }];
                const [rawKey] = hdmock.mockGET(
                    hdClient.options, 'bestObjEver', mockOptions);

                hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, httpReply) => {
                        assert.ok(httpReply);

                        // Sanity checks before buffering the stream
                        assert.strictEqual(httpReply.statusCode,
                                           mockOptions[0].statusCode);
                        assert.strictEqual(httpReply.headers['content-length'],
                                           content.length);

                        const topic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            topic, undefined);

                        // Buffer the whole stream and perform
                        // checks on 'end' event
                        const readBufs = [];
                        httpReply.on('data', function (chunk) {
                            readBufs.push(chunk);
                        });
                        httpReply.on('end', function () {
                            const buf = readBufs.join('');
                            assert.strictEqual(buf.length, content.length);
                            assert.strictEqual(buf, content);
                            done(err);
                        });
                    });
            });

            mocha.it('1 OK, 1 straggler (not timeout)', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 2,
                    code: 'CP',
                    nData: 2,
                });
                const content = 'Je suis une mite en pullover';
                const mockOptions = [
                    {
                        statusCode: 200,
                        payload: content,
                        acceptType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        acceptType: 'data',
                        timeoutMs: hdClient.options.requestTimeoutMs - 1,
                    }];
                const [rawKey] = hdmock.mockGET(
                    hdClient.options, 'bestObjEver', mockOptions);

                const opCtx = hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, httpReply) => {
                        assert.ifError(err);
                        assert.ok(httpReply);

                        // Sanity checks before buffering the stream
                        assert.strictEqual(httpReply.statusCode,
                                           mockOptions[0].statusCode);
                        assert.strictEqual(httpReply.headers['content-length'],
                                           content.length);

                        // Buffer the whole stream and perform checks
                        // on 'end' event
                        const readBufs = [];
                        httpReply.on('data', function (chunk) {
                            readBufs.push(chunk);
                        });
                        httpReply.on('end', function () {
                            const buf = readBufs.join('');
                            assert.strictEqual(buf.length, content.length);
                            assert.strictEqual(buf, content);
                        });
                    });

                /* Force waiting for all fragment ops to be over
                 * Note: the first 200 responds to the client
                 * but we must hcekc fo rthe cleanup
                 */
                function verifyEnd() {
                    if (opCtx.nPending > 0) {
                        setTimeout(verifyEnd, 1);
                        return;
                    }

                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);
                    done();
                }

                verifyEnd();
            });

            mocha.it('1 OK, 1 500', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 2,
                    code: 'CP',
                    nData: 2,
                });
                const content = 'Je suis une mite en pullover';
                const mockOptions = [
                    {
                        statusCode: 500,
                        payload: content,
                        acceptType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        acceptType: 'data',
                    }];
                const [rawKey] = hdmock.mockGET(
                    hdClient.options, 'bestObjEver', mockOptions);

                const opCtx = hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, httpReply) => {
                        assert.ok(httpReply);

                        assert.strictEqual(opCtx.status[0].nOk, 1);
                        assert.strictEqual(opCtx.status[0].nError, 1);
                        assert.strictEqual(opCtx.status[0].nTimeout, 0);
                        assert.ok(!opCtx.status[0].statuses[1].error);
                        assert.strictEqual(
                            500,
                            opCtx.status[0].statuses[0]
                                .error.infos.status, 500);

                        // Sanity checks before buffering the stream
                        assert.strictEqual(httpReply.statusCode, 200);
                        assert.strictEqual(httpReply.headers['content-length'],
                                           content.length);

                        const topic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            topic, undefined);

                        // Buffer the whole stream and perform checks
                        // on 'end' event
                        const readBufs = [];
                        httpReply.on('data', function (chunk) {
                            readBufs.push(chunk);
                        });
                        httpReply.on('end', function () {
                            const buf = readBufs.join('');
                            assert.strictEqual(buf.length, content.length);
                            assert.strictEqual(buf, content);
                            done(err);
                        });
                    });
            });

            mocha.it('1 OK, 1 404', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 2,
                    code: 'CP',
                    nData: 2,
                });
                const content = 'Je suis une mite en pullover';
                const mockOptions = [
                    {
                        statusCode: 200,
                        payload: content,
                        acceptType: 'data',
                    },
                    {
                        statusCode: 404,
                        payload: content,
                        acceptType: 'data',
                    }];
                const [rawKey] = hdmock.mockGET(
                    hdClient.options, 'bestObjEver', mockOptions);

                const opCtx = hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, httpReply) => {
                        assert.ifError(err);
                        assert.ok(httpReply);

                        // Sanity checks before buffering the stream
                        assert.strictEqual(httpReply.statusCode, 200);
                        assert.strictEqual(httpReply.headers['content-length'],
                                           content.length);

                        // Buffer the whole stream and perform checks
                        // on 'end' event
                        const readBufs = [];
                        httpReply.on('data', function (chunk) {
                            readBufs.push(chunk);
                        });
                        httpReply.on('end', function () {
                            const buf = readBufs.join('');
                            assert.strictEqual(buf.length, content.length);
                            assert.strictEqual(buf, content);
                        });
                    });

                /* Force waiting for all fragment ops to be over
                 * Note: the first 200 responds to the client
                 * but we must hcekc fo rthe cleanup
                 */
                function verifyEnd() {
                    if (opCtx.nPending > 0) {
                        setTimeout(verifyEnd, 1);
                        return;
                    }

                    assert.strictEqual(opCtx.status[0].nOk, 1);
                    assert.strictEqual(opCtx.status[0].nError, 1);
                    assert.strictEqual(opCtx.status[0].nTimeout, 0);
                    assert.ok(!opCtx.status[0].statuses[0].error);
                    assert.strictEqual(
                        opCtx.status[0].statuses[1].error.infos.status, 404);
                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, [{
                            rawKey,
                            fragments: [[0, 1]],
                        }]);
                    done();
                }

                verifyEnd();
            });

            mocha.it('All errors', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 2,
                    code: 'CP',
                    nData: 2,
                });
                const content = 'Je suis une mite en pullover';
                const mockOptions = [
                    {
                        statusCode: 404,
                        payload: content,
                        acceptType: 'data',
                    },
                    {
                        statusCode: 404,
                        payload: content,
                        acceptType: 'data',
                    }];
                const [rawKey] = hdmock.mockGET(
                    hdClient.options, 'bestObjEver', mockOptions);

                hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, httpReply) => {
                        assert.ok(err);
                        assert.ok(!httpReply);
                        assert.strictEqual(err.infos.status, 404);

                        const topic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            topic, [{
                                rawKey,
                                fragments: [[0, 0], [0, 1]],
                            }]);
                        done();
                    });
            });

            mocha.it('Worst errors selection', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 2,
                    code: 'CP',
                    nData: 2,
                });
                const content = 'Je suis une mite en pullover';
                const mockOptions = [
                    {
                        statusCode: 404,
                        payload: content,
                        acceptType: 'data',
                    },
                    {
                        statusCode: 500,
                        payload: content,
                        acceptType: 'data',
                    }];
                const [rawKey] = hdmock.mockGET(
                    hdClient.options, 'bestObjEver', mockOptions);

                hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, httpReply) => {
                        assert.ok(err);
                        assert.ok(!httpReply);
                        assert.strictEqual(err.infos.status, 500);

                        const topic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            topic, [{
                                rawKey,
                                fragments: [[0, 1]],
                            }]);
                        done();
                    });
            });
        });
    });

    mocha.describe('Persisting error edge cases', function () {
        mocha.it('All errors: failed to persit', function (done) {
            const hdClient = hdmock.getDefaultClient({
                nLocations: 2,
                code: 'CP',
                nData: 2,
            });
            const content = 'Je suis une mite en pullover';
            const mockOptions = [
                {
                    statusCode: 404,
                    payload: content,
                    acceptType: 'data',
                },
                {
                    statusCode: 404,
                    payload: content,
                    acceptType: 'data',
                }];
            const [rawKey] = hdmock.mockGET(
                hdClient.options, 'bestObjEver', mockOptions);

            hdClient.errorAgent.nextError = new Error('Demo effect!');

            hdClient.get(
                rawKey, undefined /* range */, '1',
                (err, httpReply) => {
                    /* Check failure to persist to-repair fragments */
                    assert.ok(!httpReply);
                    assert.strictEqual(err.infos.status, 500);
                    assert.strictEqual(err.message, 'Demo effect!');
                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);
                    done();
                });
        });

        mocha.it('Success but failed to persit', function (done) {
            /* Slightly different scenario: we replied to the client
             * but there was fragments to repair. And we failed to persist them...
             * NOTE: idk how/if we can handle this case
             */
            const hdClient = hdmock.getDefaultClient({
                nLocations: 2,
                code: 'CP',
                nData: 2,
            });
            const content = 'Je suis une mite en pullover';
            const mockOptions = [
                {
                    statusCode: 200,
                    payload: content,
                    acceptType: 'data',
                },
                {
                    statusCode: 404,
                    payload: content,
                    acceptType: 'data',
                }];
            const [rawKey] = hdmock.mockGET(
                hdClient.options, 'bestObjEver', mockOptions);

            hdClient.errorAgent.nextError = new Error('Demo effect!');

            const opCtx = hdClient.get(
                rawKey, undefined /* range */, '1',
                (err, httpReply) => {
                    assert.ifError(err);
                    assert.ok(httpReply);

                    // Sanity checks before buffering the stream
                    assert.strictEqual(httpReply.statusCode, 200);
                    assert.strictEqual(httpReply.headers['content-length'],
                                       content.length);

                    // Buffer the whole stream and perform checks
                    // on 'end' event
                    const readBufs = [];
                    httpReply.on('data', function (chunk) {
                        readBufs.push(chunk);
                    });
                    httpReply.on('end', function () {
                        const buf = readBufs.join('');
                        assert.strictEqual(buf.length, content.length);
                        assert.strictEqual(buf, content);
                    });
                });

            /* Force waiting for all fragment ops to be over
             * Note: the first 200 responds to the client
             * but we must hcekc fo rthe cleanup
             */
            function verifyEnd() {
                if (opCtx.nPending > 0) {
                    setTimeout(verifyEnd, 1);
                    return;
                }

                assert.strictEqual(opCtx.status[0].nOk, 1);
                assert.strictEqual(opCtx.status[0].nError, 1);
                assert.strictEqual(opCtx.status[0].nTimeout, 0);
                assert.ok(!opCtx.status[0].statuses[0].error);
                assert.strictEqual(
                    opCtx.status[0].statuses[1].error.infos.status, 404);
                const topic = hdmock.getTopic(hdClient, repairTopic);
                hdmock.strictCompareTopicContent(
                    topic, undefined);
                assert.strictEqual(opCtx.failedToPersist, true);
                done();
            }

            verifyEnd();
        });
    });
});
