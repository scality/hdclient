'use strict'; // eslint-disable-line strict
/* eslint-disable max-len */
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');
const nock = require('nock');
const fs = require('fs');
const crypto = require('crypto');
const stream = require('stream');
const ecstream = require('ecstream');

const hdclient = require('../../index');
const hdmock = require('../utils');

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
            const { rawKey } = hdmock.mockGET(
                hdClient, 'bestObjEver', content.length, [[mockOptions]]);

            hdClient.get(
                rawKey, undefined /* range */, '1',
                (err, reply) => {
                    assert.ok(reply);

                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);

                    // Buffer the whole stream and perform checks on 'end' event
                    const readBufs = [];
                    reply.on('data', function (chunk) {
                        readBufs.push(chunk);
                    });
                    reply.once('end', function () {
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
            /* Random payload contains the CRC */
            const content = fs.createReadStream(
                'tests/functional/random_payload');
            /* MD5 of file without ending CRCs (size - 12 bytes) */
            const expectedDigest = '2b7a12623e736ee1773fc3efc6c289e8';
            const mockOptions = {
                statusCode: 200,
                payload: content,
                acceptType: 'data',
            };
            const { rawKey } = hdmock.mockGET(
                hdClient, 'bestObjEver',
                hdmock.getPayloadLength(content),
                [[mockOptions]]);

            hdClient.get(
                rawKey, undefined /* range */, '1',
                (err, reply) => {
                    assert.ok(reply);

                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);

                    // Compute return valud md5
                    const hash = crypto.createHash('md5');
                    reply.on('data', function (data) {
                        hash.update(data, 'utf8');
                    });
                    reply.once('end', function () {
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
            const { rawKey } = hdmock.mockGET(
                hdClient, 'bestObjEver', content.length, [[mockOptions]]);

            hdClient.get(
                rawKey, range, '1',
                (err, reply) => {
                    assert.ifError(err);
                    assert.ok(reply);

                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);

                    // Buffer the whole stream and perform checks on 'end' event
                    const readBufs = [];
                    reply.on('data', function (chunk) {
                        readBufs.push(chunk);
                    });
                    reply.once('end', function () {
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
            const { rawKey } = hdmock.mockGET(
                hdClient, 'bestObjEver', content.length, [[mockOptions]]);

            hdClient.get(
                rawKey, range, '1',
                (err, reply) => {
                    assert.ifError(err);
                    assert.ok(reply);

                    // Buffer the whole stream and perform checks on 'end' event
                    const readBufs = [];
                    reply.on('data', function (chunk) {
                        readBufs.push(chunk);
                    });
                    reply.once('end', function () {
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
            const { rawKey } = hdmock.mockGET(
                hdClient, 'bestObjEver', content.length, [[mockOptions]]);

            hdClient.get(
                rawKey, range, '1',
                (err, reply) => {
                    assert.ok(reply);

                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);

                    // Buffer the whole stream and perform checks on 'end' event
                    const readBufs = [];
                    reply.on('data', function (chunk) {
                        readBufs.push(chunk);
                    });
                    reply.once('end', function () {
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
            const { rawKey } = hdmock.mockGET(
                hdClient, 'bestObjEver', content.length, [[mockOptions]]);

            hdClient.get(
                rawKey, range, '1',
                (err, reply) => {
                    assert.ok(reply);

                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);

                    // Buffer the whole stream and perform checks on 'end' event
                    const readBufs = [];
                    reply.on('data', function (chunk) {
                        readBufs.push(chunk);
                    });
                    reply.once('end', function () {
                        const buf = readBufs.join('');
                        assert.strictEqual(buf.length, expectedContent.length);
                        assert.strictEqual(buf, expectedContent);
                        done(err);
                    });
                });
        });

        mocha.it('Stupid range', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const content = 'Je suis une mite en pullover';
            const { rawKey } = hdmock.mockGET(
                hdClient, 'bestObjEver', content.length, [[{}]]);

            hdClient.get(
                rawKey, [999999, 9999999999], '1',
                err => {
                    assert.ok(err);
                    assert.ok(err.message.startsWith('Invalid range'));
                    done();
                });
        });

        mocha.it('Not found key', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const { rawKey } = hdmock.mockGET(
                hdClient,
                'bestObjEver',
                1024,
                [[{
                    statusCode: 404,
                    payload: '',
                    acceptType: 'data',
                }]]
            );

            hdClient.get(rawKey, null /* range */, '1', err => {
                assert.ok(err);
                assert.strictEqual(err.code, 404);

                const topic = hdmock.getTopic(hdClient, repairTopic);
                hdmock.strictCompareTopicContent(
                    topic, [{
                        rawKey,
                        fragments: [[0, 0]],
                        version: 1,
                    }]);
                done();
            });
        });

        mocha.it('Server error', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const { rawKey } = hdmock.mockGET(
                hdClient,
                'bestObjEver',
                1024,
                [[{
                    statusCode: 500,
                    payload: '',
                    acceptType: 'data',
                }]]
            );

            hdClient.get(rawKey, null /* range */, '1', err => {
                assert.ok(err);
                assert.strictEqual(err.code, 500);
                const topic = hdmock.getTopic(hdClient, repairTopic);
                hdmock.strictCompareTopicContent(
                        topic, undefined);
                done();
            });
        });

        mocha.it('Bad key', function (done) {
            const hdClient = hdmock.getDefaultClient();
            hdClient.delete('---', '1', err => {
                if (!(err instanceof Error)) {
                    throw err;
                }
                assert.strictEqual(err.message, 'ParseError');
                assert.strictEqual(err.code, 400);
                done();
            });
        });

        mocha.it('Timeout', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const mockDelay = hdClient.options.requestTimeoutMs + 10;
            const { rawKey } = hdmock.mockGET(
                hdClient,
                'bestObjEver',
                314159,
                [[{
                    statusCode: 200,
                    payload: 'gné',
                    acceptType: 'data',
                    timeoutMs: mockDelay,
                }]]
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

        mocha.it('Corrupted', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const content = 'Je suis une mite en pullover';
            const mockOptions = {
                statusCode: 200,
                payload: content,
                acceptType: 'data',
                storedCRC: 0x1234,
                actualCRC: 0xdead,
            };
            const { rawKey } = hdmock.mockGET(
                hdClient, 'bestObjEver', content.length, [[mockOptions]]);

            hdClient.get(
                rawKey, undefined /* range */, '1',
                (err, reply) => {
                    /* Everything is green when starting to read... */

                    /* Nothing to repair yet */
                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);

                    // Use up the whole stream, expects error before end
                    reply.on('error', err => {
                        assert.strictEqual(err.message, 'CorruptedData');
                        assert.strictEqual(err.code, 422);
                        assert.strictEqual(err.description, 'Bad CRC');
                        assert.strictEqual(err.infos.method, 'GET');
                        assert.strictEqual(err.infos.actualCRC, mockOptions.actualCRC);
                        assert.strictEqual(err.infos.expectedCRC, mockOptions.storedCRC);

                        /* 1 fragment to repair, eventually */
                        setTimeout(() => {
                            const topic = hdmock.getTopic(
                                hdClient, repairTopic);
                            hdmock.strictCompareTopicContent(
                                topic,
                                [{
                                    rawKey,
                                    fragments: [[0, 0]],
                                    version: 1,
                                }]);
                            done();
                        }, 10);
                    });
                });
        });
    });

    mocha.describe('Multiple hyperdrives', function () {
        mocha.describe('Replication', function () {
            mocha.it('All success', function (done) {
                const codes = [{ type: 'CP', dataParts: 2, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 2, codes });
                const content = 'Je suis une mite en pullover';
                const mockOptions = [[
                    {
                        statusCode: 200,
                        payload: content,
                        acceptType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        acceptType: 'data',
                    }]];
                const { rawKey } = hdmock.mockGET(
                    hdClient, 'bestObjEver', content.length, mockOptions);

                hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, reply) => {
                        assert.ok(reply);

                        const topic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            topic, undefined);

                        // Buffer the whole stream and perform
                        // checks on 'end' event
                        const readBufs = [];
                        reply.on('data', function (chunk) {
                            readBufs.push(chunk);
                        });
                        reply.once('end', function () {
                            const buf = readBufs.join('');
                            assert.strictEqual(buf.length, content.length);
                            assert.strictEqual(buf, content);
                            done(err);
                        });
                    });
            });

            mocha.it('1 OK, 1 straggler (not timeout)', function (done) {
                const codes = [{ type: 'CP', dataParts: 2, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 2, codes });
                const content = 'Je suis une mite en pullover';
                const mockOptions = [[
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
                    }]];
                const { rawKey } = hdmock.mockGET(
                    hdClient, 'bestObjEver', content.length, mockOptions);

                const opCtx = hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, reply) => {
                        assert.ifError(err);
                        assert.ok(reply);

                        // Buffer the whole stream and perform checks
                        // on 'end' event
                        const readBufs = [];
                        reply.on('data', function (chunk) {
                            readBufs.push(chunk);
                        });
                        reply.once('end', function () {
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
                const codes = [{ type: 'CP', dataParts: 2, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 2, codes });
                const content = 'Je suis une mite en pullover';
                const mockOptions = [[
                    {
                        statusCode: 500,
                        payload: content,
                        acceptType: 'data',
                    },
                    {
                        statusCode: 200,
                        payload: content,
                        acceptType: 'data',
                    }]];
                const { rawKey } = hdmock.mockGET(
                    hdClient, 'bestObjEver', content.length, mockOptions);

                const opCtx = hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, reply) => {
                        assert.ok(reply);

                        assert.strictEqual(opCtx.status[0].nOk, 1);
                        assert.strictEqual(opCtx.status[0].nError, 1);
                        assert.strictEqual(opCtx.status[0].nTimeout, 0);
                        assert.ok(!opCtx.status[0].statuses[1].error);
                        assert.strictEqual(
                            500,
                            opCtx.status[0].statuses[0]
                                .error.code, 500);

                        const topic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            topic, undefined);

                        // Buffer the whole stream and perform checks
                        // on 'end' event
                        const readBufs = [];
                        reply.on('data', function (chunk) {
                            readBufs.push(chunk);
                        });
                        reply.once('end', function () {
                            const buf = readBufs.join('');
                            assert.strictEqual(buf.length, content.length);
                            assert.strictEqual(buf, content);
                            done(err);
                        });
                    });
            });

            mocha.it('1 OK, 1 404', function (done) {
                const codes = [{ type: 'CP', dataParts: 2, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 2, codes });
                const content = 'Je suis une mite en pullover';
                const mockOptions = [[
                    {
                        statusCode: 200,
                        payload: content,
                        acceptType: 'data',
                    },
                    {
                        statusCode: 404,
                        payload: content,
                        acceptType: 'data',
                    }]];
                const { rawKey } = hdmock.mockGET(
                    hdClient, 'bestObjEver', content.length, mockOptions);

                const opCtx = hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, reply) => {
                        assert.ifError(err);
                        assert.ok(reply);

                        // Buffer the whole stream and perform checks
                        // on 'end' event
                        const readBufs = [];
                        reply.on('data', function (chunk) {
                            readBufs.push(chunk);
                        });
                        reply.once('end', function () {
                            const buf = readBufs.join('');
                            assert.strictEqual(buf.length, content.length);
                            assert.strictEqual(buf, content);
                        });
                    });

                /* Force waiting for all fragment ops to be over
                 * Note: the first 200 responds to the client
                 * but we must wait for the cleanup
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
                        opCtx.status[0].statuses[1].error.code, 404);
                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, [{
                            rawKey,
                            fragments: [[0, 1]],
                            version: 1,
                        }]);
                    done();
                }

                setTimeout(verifyEnd, 10);
            });

            mocha.it('All errors', function (done) {
                const codes = [{ type: 'CP', dataParts: 2, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 2, codes });
                const content = 'Je suis une mite en pullover';
                const mockOptions = [[
                    {
                        statusCode: 404,
                        payload: content,
                        acceptType: 'data',
                    },
                    {
                        statusCode: 404,
                        payload: content,
                        acceptType: 'data',
                    }]];
                const { rawKey } = hdmock.mockGET(
                    hdClient, 'bestObjEver', content.length, mockOptions);

                hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, reply) => {
                        assert.ok(err);
                        assert.ok(!reply);
                        assert.strictEqual(err.code, 404);

                        const topic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            topic, [{
                                rawKey,
                                fragments: [[0, 0], [0, 1]],
                                version: 1,
                            }]);
                        done();
                    });
            });

            mocha.it('Worst errors selection', function (done) {
                const codes = [{ type: 'CP', dataParts: 2, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 2, codes });
                const content = 'Je suis une mite en pullover';
                const mockOptions = [[
                    {
                        statusCode: 404,
                        payload: content,
                        acceptType: 'data',
                    },
                    {
                        statusCode: 500,
                        payload: content,
                        acceptType: 'data',
                    }]];
                const { rawKey } = hdmock.mockGET(
                    hdClient, 'bestObjEver', content.length, mockOptions);

                hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, reply) => {
                        assert.ok(err);
                        assert.ok(!reply);
                        assert.strictEqual(err.code, 500);

                        const topic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            topic, [{
                                rawKey,
                                fragments: [[0, 0]],
                                version: 1,
                            }]);
                        done();
                    });
            });
        });

        mocha.describe('Erasure coding', function () {
            const code = 'RS';
            [[2, 1], [4, 2], [5, 6], [7, 5]].forEach(args => {
                const [k, m] = args;
                hdclient.utils.range(m).forEach(missing => {
                    [hdclient.split.DATA_ALIGN / 2, // Less than a stripe, requires 0-padding
                     k * hdclient.split.DATA_ALIGN, // Exactly 1 stripe
                     k * hdclient.split.DATA_ALIGN + 23, // 1 full stripe, 0-padding the rest
                    ].forEach(size => {
                        [false, true].forEach(corrupt => {
                            const killed = new Map();
                            while (killed.size < missing) {
                                killed.set(Math.floor(Math.random() * (k + m)), 1);
                            }

                            const description = `size=${size}, k=${k}, m=${m}, missing=${missing}, corrupted=${corrupt}`;
                            mocha.it(`Success (${description})`, function (done) {
                                const codes = [{
                                    type: 'RS',
                                    dataParts: k,
                                    codingParts: m,
                                    pattern: '.*',
                                }];
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
                                    let corrupted = false;
                                    const mocks = [buffers.map((b, i) => {
                                        const shouldKill = killed.has(i);
                                        const shouldCorrupt = corrupt && !corrupted;
                                        corrupted |= shouldCorrupt;
                                        return {
                                            statusCode: (shouldCorrupt || !shouldKill) ? 200 : 404,
                                            payload: b,
                                            acceptType: 'data',
                                            storedCRC: 0x1234,
                                            actualCRC: shouldCorrupt ? 0xdead : 0x1234,
                                        };
                                    })];
                                    const { rawKey } = hdmock.mockGET(
                                        hdClient, 'bestObjEver', content.length, mocks);

                                    const topicEndCheck = () => {
                                        const topic = hdmock.getTopic(hdClient, repairTopic);
                                        let expectedRepairTopic = undefined;
                                        if (missing > 0 || corrupt) {
                                            if (corrupt) {
                                                killed.set(0, 1); // Add corrupted fragment
                                            }
                                            expectedRepairTopic = [{
                                                rawKey,
                                                fragments: [...killed.keys()].sort((a, b) => (a - b))
                                                    .map(f => [0, f]),
                                                version: 1,
                                            }];
                                        }
                                        hdmock.strictCompareTopicContent(
                                            topic, expectedRepairTopic);
                                        done();
                                    };

                                    hdClient.get(
                                        rawKey, undefined /* range */, '1',
                                        (err, reply) => {
                                            assert.ifError(err);
                                            assert.ok(reply);

                                            // Buffer the whole stream and perform
                                            // checks on 'end' event
                                            const readBufs = [];
                                            reply.on('data', function (chunk) {
                                                readBufs.push(chunk);
                                            });
                                            reply.once('end', function () {
                                                const buf = Buffer.concat(readBufs);
                                                assert.strictEqual(buf.length, content.length);
                                                assert.strictEqual(0, Buffer.compare(buf, content));
                                                topicEndCheck();
                                            });
                                            reply.once('error', err => {
                                                if (!corrupt) {
                                                    done(err);
                                                    return;
                                                }
                                                assert.strictEqual(err.message, 'CorruptedData');
                                                assert.strictEqual(err.code, 422);
                                                setImmediate(() => topicEndCheck());
                                            });
                                        });
                                }).catch(err => done(err));
                            });
                        });
                    });
                });
            });

            mocha.it('All errors', function (done) {
                const [k, m] = [4, 2];
                const codes = [{ type: 'RS', dataParts: k, codingParts: m, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: k + m, codes });
                const mockOptions = [hdclient.utils.range(k + m).map(() => ({
                    statusCode: 404,
                    payload: 'meh',
                    acceptType: 'data',
                }))];
                const { rawKey } = hdmock.mockGET(
                    hdClient, 'bestObjEver', 42, mockOptions);

                hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, reply) => {
                        assert.ok(err);
                        assert.ok(!reply);
                        assert.strictEqual(err.code, 404);

                        const topic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            topic, [{
                                rawKey,
                                fragments: hdclient.utils.range(k + m).map(i => [0, i]),
                                version: 1,
                            }]);
                        done();
                    });
            });
        });
    });

    mocha.describe('Persisting error edge cases', function () {
        mocha.it('All errors: failed to persist', function (done) {
            const codes = [{ type: 'CP', dataParts: 2, codingParts: 0, pattern: '.*' }];
            const hdClient = hdmock.getDefaultClient({ nLocations: 2, codes });
            const content = 'Je suis une mite en pullover';
            const mockOptions = [[
                {
                    statusCode: 404,
                    payload: content,
                    acceptType: 'data',
                },
                {
                    statusCode: 404,
                    payload: content,
                    acceptType: 'data',
                }]];
            const { rawKey } = hdmock.mockGET(
                hdClient, 'bestObjEver', content.length, mockOptions);

            hdClient.errorAgent.nextError = new Error('Demo effect!');

            hdClient.get(
                rawKey, undefined /* range */, '1',
                (err, reply) => {
                    /* Check failure to persist to-repair fragments */
                    assert.ok(!reply);
                    assert.strictEqual(err.code, 500);
                    assert.strictEqual(err.message, 'InternalError');
                    assert.strictEqual(
                        err.description,
                        'Failed to persist fragments to repair: Demo effect!');
                    const topic = hdmock.getTopic(hdClient, repairTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);
                    done();
                });
        });

        mocha.it('Success but failed to persist', function (done) {
            /* Slightly different scenario: we replied to the client
             * but there was fragments to repair. And we failed to
             * persist them...
             * NOTE: idk how/if we can handle this case
             */
            const codes = [{ type: 'CP', dataParts: 2, codingParts: 0, pattern: '.*' }];
            const hdClient = hdmock.getDefaultClient({ nLocations: 2, codes });
            const content = 'Je suis une mite en pullover';
            const mockOptions = [[
                {
                    statusCode: 200,
                    payload: content,
                    acceptType: 'data',
                },
                {
                    statusCode: 404,
                    payload: content,
                    acceptType: 'data',
                }]];
            const { rawKey } = hdmock.mockGET(
                hdClient, 'bestObjEver', content.length, mockOptions);

            hdClient.errorAgent.nextError = new Error('Demo effect!');

            const opCtx = hdClient.get(
                rawKey, undefined /* range */, '1',
                (err, reply) => {
                    assert.ifError(err);
                    assert.ok(reply);

                    // Buffer the whole stream and perform checks
                    // on 'end' event
                    const readBufs = [];
                    reply.on('data', function (chunk) {
                        readBufs.push(chunk);
                    });
                    reply.once('end', function () {
                        const buf = readBufs.join('');
                        assert.strictEqual(buf.length, content.length);
                        assert.strictEqual(buf, content);
                    });
                });

            /* Force waiting for all fragment ops to be over
             * Note: the first 200 responds to the client
             * but we must check for the cleanup
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
                    opCtx.status[0].statuses[1].error.code, 404);
                const topic = hdmock.getTopic(hdClient, repairTopic);
                hdmock.strictCompareTopicContent(
                    topic, undefined);
                assert.strictEqual(opCtx.failedToPersist, true);
                done();
            }

            setTimeout(verifyEnd, 10);
        });
    });

    mocha.describe('Split', function () {
        mocha.describe('Replication', function () {
            mocha.it('All success', function (done) {
                const content = crypto.randomBytes(30000).toString('ascii');
                const size = hdmock.getPayloadLength(content);
                const minSplitSize = size / 3;
                const realSplitSize = hdclient.split.align(
                    minSplitSize, hdclient.split.DATA_ALIGN);
                assert.ok(size > realSplitSize);
                const codes = [{ type: 'CP', dataParts: 2, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ minSplitSize, codes, nLocations: 2 });
                const mockOptions = [
                    [{
                        statusCode: 200,
                        payload: content.slice(0, realSplitSize),
                        acceptType: 'data',
                    }, {
                        statusCode: 200,
                        payload: content.slice(0, realSplitSize),
                        acceptType: 'data',
                    }],
                    [{
                        statusCode: 200,
                        payload: content.slice(realSplitSize, 2 * realSplitSize),
                        acceptType: 'data',
                    }, {
                        statusCode: 200,
                        payload: content.slice(realSplitSize, 2 * realSplitSize),
                        acceptType: 'data',
                    }],
                    [{
                        statusCode: 200,
                        payload: content.slice(2 * realSplitSize),
                        acceptType: 'data',
                    }, {
                        statusCode: 200,
                        payload: content.slice(2 * realSplitSize),
                        acceptType: 'data',
                    }],
                ];
                const { rawKey } = hdmock.mockGET(
                    hdClient, 'bestObjEver', content.length, mockOptions);

                hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, reply) => {
                        assert.ifError(err);

                        const topic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            topic, undefined);

                        // Buffer the whole stream and perform
                        // checks on 'end' event
                        const readBufs = [];
                        reply.on('data', function (chunk) {
                            readBufs.push(chunk);
                        });
                        reply.once('error', err => done(err));
                        reply.once('end', function () {
                            const buf = readBufs.join('');
                            assert.strictEqual(buf.length, content.length);
                            assert.strictEqual(buf, content);
                            done();
                        });
                    });
            });

            mocha.it('Recoverable sprinkled errors', function (done) {
                const content = crypto.randomBytes(30000).toString('ascii');
                const size = hdmock.getPayloadLength(content);
                const minSplitSize = size / 3;
                const realSplitSize = hdclient.split.align(
                    minSplitSize, hdclient.split.DATA_ALIGN);
                assert.ok(size > realSplitSize);
                const codes = [{ type: 'CP', dataParts: 2, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ minSplitSize, codes, nLocations: 2 });
                const mockOptions = [
                    [{
                        statusCode: 200,
                        payload: content.slice(0, realSplitSize),
                        acceptType: 'data',
                    }, {
                        statusCode: 404,
                        payload: content.slice(0, realSplitSize),
                        acceptType: 'data',
                    }],
                    [{
                        statusCode: 200,
                        payload: content.slice(realSplitSize, 2 * realSplitSize),
                        acceptType: 'data',
                    }, {
                        statusCode: 200,
                        payload: content.slice(realSplitSize, 2 * realSplitSize),
                        acceptType: 'data',
                        //timeoutMs: hdClient.options.requestTimeoutMs + 10,
                    }],
                    [{
                        statusCode: 500,
                        payload: content.slice(2 * realSplitSize),
                        acceptType: 'data',
                    }, {
                        statusCode: 200,
                        payload: content.slice(2 * realSplitSize),
                        acceptType: 'data',
                    }],
                ];
                const { rawKey } = hdmock.mockGET(
                    hdClient, 'bestObjEver', content.length, mockOptions);

                hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, reply) => {
                        assert.ifError(err);

                        // Buffer the whole stream and perform
                        // checks on 'end' event
                        const readBufs = [];
                        reply.on('data', function (chunk) {
                            readBufs.push(chunk);
                        });
                        reply.once('error', err => done(err));
                        reply.once('end', function () {
                            const buf = readBufs.join('');
                            assert.strictEqual(buf.length, content.length);
                            assert.strictEqual(buf, content);

                            const topic = hdmock.getTopic(hdClient, repairTopic);
                            hdmock.strictCompareTopicContent(
                                topic, [{
                                    rawKey,
                                    fragments: [[0, 1]],
                                    version: 1,
                                }]);
                            done();
                        });
                    });
            });

            mocha.it('Corruption in middle chunk', function (done) {
                const content = crypto.randomBytes(30000).toString('ascii');
                const size = hdmock.getPayloadLength(content);
                const minSplitSize = size / 3;
                const realSplitSize = hdclient.split.align(
                    minSplitSize, hdclient.split.DATA_ALIGN);
                assert.ok(size > realSplitSize);
                const codes = [{ type: 'CP', dataParts: 2, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ minSplitSize, codes, nLocations: 2 });
                const mockOptions = [
                    [{
                        statusCode: 200,
                        payload: content.slice(0, realSplitSize),
                        acceptType: 'data',
                    }, {
                        statusCode: 200,
                        payload: content.slice(0, realSplitSize),
                        acceptType: 'data',
                    }],
                    [{
                        statusCode: 404,
                        payload: content.slice(realSplitSize, 2 * realSplitSize),
                        acceptType: 'data',
                    }, {
                        statusCode: 200,
                        payload: content.slice(realSplitSize, 2 * realSplitSize),
                        acceptType: 'data',
                        storedCRC: 0x1234,
                        actualCRC: 0xdead,
                    }],
                    [{
                        statusCode: 500,
                        payload: content.slice(2 * realSplitSize),
                        acceptType: 'data',
                    }, {
                        statusCode: 200,
                        payload: content.slice(2 * realSplitSize),
                        acceptType: 'data',
                    }],
                ];
                const { rawKey } = hdmock.mockGET(
                    hdClient, 'bestObjEver', content.length, mockOptions);

                hdClient.get(
                    rawKey, undefined /* range */, '1',
                    (err, reply) => {
                        /* Nothing to repair yet */
                        const topic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            topic, undefined);

                        // Use up the whole stream, expects error before end
                        reply.on('error', err => {
                            assert.strictEqual(err.message, 'CorruptedData');
                            assert.strictEqual(err.code, 422);
                            assert.strictEqual(err.infos.chunkId, 1);
                            assert.strictEqual(err.infos.fragmentId, 1);
                            assert.strictEqual(err.infos.actualCRC, mockOptions[1][1].actualCRC);
                            assert.strictEqual(err.infos.expectedCRC, mockOptions[1][1].storedCRC);

                            /* 1 fragment to repair, eventually */
                            setTimeout(() => {
                                const topic = hdmock.getTopic(
                                    hdClient, repairTopic);
                                hdmock.strictCompareTopicContent(
                                    topic,
                                    [{
                                        rawKey,
                                        fragments: [[1, 0], [1, 1]],
                                        version: 1,
                                    }]);
                                done();
                            }, 10);
                        });
                    });
            });

            mocha.it('Full range', function (done) {
                const content = crypto.randomBytes(30000).toString('ascii');
                const range = [100, 29000];
                // Slice does not include right boundary, while HTTP ranges are inclusive
                const expectedContent = content.slice(range[0], range[1] + 1);
                const size = hdmock.getPayloadLength(content);
                const minSplitSize = size / 3;
                const realSplitSize = hdclient.split.align(
                    minSplitSize, hdclient.split.DATA_ALIGN);
                assert.ok(size > realSplitSize);

                const codes = [{ type: 'CP', dataParts: 2, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ minSplitSize, codes, nLocations: 2 });
                const mockOptions = [
                    [{
                        statusCode: 200,
                        payload: content.slice(0, realSplitSize),
                        acceptType: 'data',
                        range: [100, realSplitSize - 1],
                    }, {
                        statusCode: 200,
                        payload: content.slice(0, realSplitSize),
                        acceptType: 'data',
                        range: [100, realSplitSize - 1],
                    }],
                    [{
                        statusCode: 200,
                        payload: content.slice(realSplitSize, 2 * realSplitSize),
                        acceptType: 'data',
                        range: [realSplitSize, 2 * realSplitSize - 1],
                    }, {
                        statusCode: 200,
                        payload: content.slice(realSplitSize, 2 * realSplitSize),
                        acceptType: 'data',
                        range: [realSplitSize, 2 * realSplitSize - 1],
                    }],
                    [{
                        statusCode: 200,
                        payload: content.slice(2 * realSplitSize),
                        acceptType: 'data',
                        range: [2 * realSplitSize, 29000],
                    }, {
                        statusCode: 200,
                        payload: content.slice(2 * realSplitSize),
                        acceptType: 'data',
                        range: [2 * realSplitSize, 29000],
                    }],
                ];
                const { rawKey } = hdmock.mockGET(
                    hdClient, 'bestObjEver', content.length, mockOptions);

                hdClient.get(
                    rawKey, range, '1',
                    (err, reply) => {
                        assert.ifError(err);

                        const topic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            topic, undefined);

                        // Buffer the whole stream and perform
                        // checks on 'end' event
                        const readBufs = [];
                        reply.on('data', function (chunk) {
                            readBufs.push(chunk);
                        });
                        reply.once('error', err => done(err));
                        reply.once('end', function () {
                            const buf = readBufs.join('');
                            assert.strictEqual(buf.length, expectedContent.length);
                            assert.strictEqual(buf, expectedContent);
                            done();
                        });
                    });
            });

            mocha.it('Half-range', function (done) {
                const content = crypto.randomBytes(30000).toString('ascii');
                const size = hdmock.getPayloadLength(content);
                const minSplitSize = size / 3;
                const realSplitSize = hdclient.split.align(
                    minSplitSize, hdclient.split.DATA_ALIGN);
                assert.ok(size > realSplitSize);
                const range = [realSplitSize];
                const codes = [{ type: 'CP', dataParts: 2, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ minSplitSize, codes, nLocations: 2 });
                const mockOptions = [
                    [{}, {}], // Should not be called
                    [{
                        statusCode: 200,
                        payload: content.slice(realSplitSize, 2 * realSplitSize),
                        acceptType: 'data',
                        range: [realSplitSize, 2 * realSplitSize - 1],
                    }, {
                        statusCode: 200,
                        payload: content.slice(realSplitSize, 2 * realSplitSize),
                        acceptType: 'data',
                        range: [realSplitSize, 2 * realSplitSize - 1],
                    }],
                    [{
                        statusCode: 200,
                        payload: content.slice(2 * realSplitSize),
                        acceptType: 'data',
                        range: [2 * realSplitSize, content.length],
                    }, {
                        statusCode: 200,
                        payload: content.slice(2 * realSplitSize),
                        acceptType: 'data',
                        range: [2 * realSplitSize, content.length],
                    }],
                ];
                const { rawKey } = hdmock.mockGET(
                    hdClient, 'bestObjEver', content.length, mockOptions);

                hdClient.get(
                    rawKey, range, '1',
                    (err, reply) => {
                        assert.ifError(err);

                        const topic = hdmock.getTopic(hdClient, repairTopic);
                        hdmock.strictCompareTopicContent(
                            topic, undefined);

                        // Buffer the whole stream and perform
                        // checks on 'end' event
                        const readBufs = [];
                        reply.on('data', function (chunk) {
                            readBufs.push(chunk);
                        });
                        reply.once('error', err => done(err));
                        reply.once('end', function () {
                            const buf = readBufs.join('');
                            assert.strictEqual(buf.length, content.slice(...range).length);
                            assert.strictEqual(buf, content.slice(...range));
                            done();
                        });
                    });
            });
        });
    });
});
