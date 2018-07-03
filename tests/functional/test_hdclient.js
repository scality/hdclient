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

function getDefaultClient() {
    const endpoint = 'hyperdrive-store1:8888';
    const conf = {
        endpoints: [endpoint],
        dataParts: 1,
        codingParts: 0,
        requestTimeoutMs: 10,
    };

    const client = new hdclient.client.HyperdriveClient(conf);
    client.logging.config.update({ level: 'fatal', dump: 'fatal' });
    return client;
}

mocha.describe('Hyperdrive Client Single endpoint suite', function () {
    // Clean all HTTP mocks before starting the test
    mocha.beforeEach(nock.cleanAll);

    // Verify all mocks have been used - no garbage
    mocha.afterEach(function () {
        assert.ok(nock.isDone);
    });

    mocha.describe('Internals', function () {
        mocha.it('Socket error handling', function (done) {
            const hdClient = getDefaultClient();
            const [ip, port] = hdClient.options.endpoints[0].split(':');
            const opts = hdClient._getCommonStoreRequestOptions(
                ip, Number(port), 'test_key'
            );
            opts.method = 'GET';
            opts.path = '/jesuisunemiteenpullover';
            const noLog = { error() {} };
            const expectedErrorMessage = 'something awful happened';

            nock(`http://${hdClient.options.endpoints[0]}`)
                 .get(opts.path)
                 .replyWithError(expectedErrorMessage);

            hdClient._newRequest(opts, noLog, err => {
                assert.strictEqual(expectedErrorMessage, err.message);
                done();
            }).end();
        });
    });

    mocha.describe('DELETE', function () {
        mocha.it('Existing key', function (done) {
            const hdClient = getDefaultClient();
            const [rawKey] = hdmock.mockDELETE(
                hdClient.options, 'bestObjEver', [[200]]);
            hdClient.delete(rawKey, '1', err => {
                done(err);
            });
        });

        mocha.it('Not found key', function (done) {
            const hdClient = getDefaultClient();
            const [rawKey] = hdmock.mockDELETE(
                hdClient.options, 'bestObjEver', [[404]]);
            hdClient.delete(rawKey, '1', err => {
                assert.strictEqual(err.infos.status, 404);
                done();
            });
        });

        mocha.it('Server error', function (done) {
            const hdClient = getDefaultClient();
            const [rawKey] = hdmock.mockDELETE(
                hdClient.options, 'bestObjEver', [[500]]);
            hdClient.delete(rawKey, '1', err => {
                assert.strictEqual(err.infos.status, 500);
                done();
            });
        });

        mocha.it('Bad key', function (done) {
            const hdClient = getDefaultClient();
            hdClient.delete('---', '1', err => {
                if (!(err instanceof BadKeyError)) {
                    throw err;
                }
                done();
            });
        });

        mocha.it('Timeout', function (done) {
            const hdClient = getDefaultClient();
            const mockDelay = hdClient.options.requestTimeoutMs + 10;
            const payloads = [[200, mockDelay]];
            const [rawKey] = hdmock.mockDELETE(
                hdClient.options, 'bestObjEver', payloads);

            hdClient.delete(rawKey, '1', err => {
                assert.ok(err);
                done();
            });
        });
    });

    mocha.describe('GET', function () {
        mocha.it('Existing small key', function (done) {
            const hdClient = getDefaultClient();
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
                    assert.ifError(err);
                    assert.ok(httpReply);

                    // Sanity checks before buffering the stream
                    assert.strictEqual(httpReply.statusCode,
                                       mockOptions.statusCode);
                    assert.strictEqual(httpReply.headers['content-length'],
                                       content.length);

                    // Buffer the whole stream and perform checks on 'end' event
                    const readBufs = [];
                    httpReply.on('data', function (chunk) {
                        readBufs.push(chunk);
                    });
                    httpReply.on('end', function () {
                        const buf = readBufs.join('');
                        assert.strictEqual(buf.length, content.length);
                        assert.strictEqual(buf, content);
                        done();
                    });
                });
        });

        mocha.it('Existing larger key (32 KiB)', function (done) {
            const hdClient = getDefaultClient();
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
                    assert.ifError(err);
                    assert.ok(httpReply);

                    // Sanity checks before buffering the stream
                    assert.strictEqual(httpReply.statusCode,
                                       mockOptions.statusCode);
                    assert.strictEqual(httpReply.headers['content-length'],
                                       contentLength);

                    // Compute return valud md5
                    const hash = crypto.createHash('md5');
                    httpReply.on('data', function (data) {
                        hash.update(data, 'utf8');
                    });
                    httpReply.on('end', function () {
                        const getDigest = hash.digest('hex');
                        assert.strictEqual(getDigest, expectedDigest);
                        done();
                    });
                });
        });

        mocha.it('Half range', function (done) {
            const hdClient = getDefaultClient();
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
                    assert.ifError(err);
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
                        done();
                    });
                });
        });

        mocha.it('Full range', function (done) {
            const hdClient = getDefaultClient();
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
                    assert.ifError(err);
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
                        done();
                    });
                });
        });

        mocha.it('Larger-than-size range', function (done) {
            const hdClient = getDefaultClient();
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
                    assert.ifError(err);
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
                        done();
                    });
                });
        });

        mocha.it('First byte only', function (done) {
            const hdClient = getDefaultClient();
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
                    assert.ifError(err);
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
                        done();
                    });
                });
        });

        mocha.it('Not found key', function (done) {
            const hdClient = getDefaultClient();
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
                done();
            });
        });

        mocha.it('Server error', function (done) {
            const hdClient = getDefaultClient();
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
                done();
            });
        });

        mocha.it('Bad key', function (done) {
            const hdClient = getDefaultClient();
            hdClient.delete('---', '1', err => {
                if (!(err instanceof BadKeyError)) {
                    throw err;
                }
                done();
            });
        });

        mocha.it('Timeout', function (done) {
            const hdClient = getDefaultClient();
            const mockDelay = hdClient.options.requestTimeoutMs + 10;
            const [rawKey] = hdmock.mockGET(
                hdClient.options,
                'bestObjEver',
                [{
                    statusCode: 500,
                    payload: '',
                    acceptType: 'data',
                    timeoutMs: mockDelay,
                }]
            );

            hdClient.get(
                rawKey, undefined /* range */, '1',
                err => {
                    assert.ok(err);
                    done();
                });
        });
    });

    mocha.describe('PUT', function () {
        mocha.it('Success small key', function (done) {
            const hdClient = getDefaultClient();
            const content = 'Je suis une mite en pullover';
            const payloads = [[200, content, 'data']];
            const keyContext = {
                objectKey: 'bestObjEver',
            };

            hdmock.mockPUT(hdClient.options, keyContext, payloads);

            hdClient.put(
                hdmock.streamString(content),
                hdmock.getPayloadLength(content),
                keyContext, '1',
                (err, rawKey) => {
                    assert.ifError(err);
                    assert.strictEqual(typeof rawKey, 'string');
                    const parts = hdclient.keyscheme.deserialize(rawKey);

                    const [endpoint, port] = hdClient.options
                              .endpoints[0].split(':');
                    assert.strictEqual(parts.nDataParts, 1);
                    assert.strictEqual(parts.nCodingParts, 0);
                    assert.strictEqual(parts.data[0].hostname, endpoint);
                    assert.strictEqual(parts.data[0].port, Number(port));
                    assert.ok(parts.data[0].key, keyContext.objectKey);
                    done();
                });
        });

        mocha.it('Success larger key (32 KiB)', function (done) {
            const hdClient = getDefaultClient();
            /* TODO avoid depending on hardcoded path */
            const content = fs.createReadStream(
                'tests/functional/random_payload');
            const payloads = [[200, content, 'data']];
            const keyContext = {
                objectKey: 'bestObjEver',
            };

            hdmock.mockPUT(hdClient.options, keyContext, payloads);

            hdClient.put(
                content,
                hdmock.getPayloadLength(content),
                keyContext, '1',
                (err, rawKey) => {
                    assert.ifError(err);
                    assert.strictEqual(typeof rawKey, 'string');
                    const parts = hdclient.keyscheme.deserialize(rawKey);

                    const [endpoint, port] = hdClient.options
                              .endpoints[0].split(':');
                    assert.strictEqual(parts.nDataParts, 1);
                    assert.strictEqual(parts.nCodingParts, 0);
                    assert.strictEqual(parts.data[0].hostname, endpoint);
                    assert.strictEqual(parts.data[0].port, Number(port));
                    assert.ok(parts.data[0].key, keyContext.objectKey);
                    done();
                });
        });

        mocha.it('Server error', function (done) {
            const hdClient = getDefaultClient();
            /* TODO avoid depending on hardcoded path */
            const content = fs.createReadStream(
                'tests/functional/random_payload');
            const payloads = [[500, content, 'data']];
            const keyContext = {
                objectKey: 'bestObjEver',
            };

            hdmock.mockPUT(hdClient.options, keyContext, payloads);

            hdClient.put(
                content,
                hdmock.getPayloadLength(content),
                keyContext, '1',
                err => {
                    assert.strictEqual(err.infos.status, 500);
                    done();
                });
        });

        mocha.it('Timeout', function (done) {
            const hdClient = getDefaultClient();
            const mockDelay = hdClient.options.requestTimeoutMs + 10;
            const content = 'Je suis une mite en pullover';
            const payloads = [[404, content, 'data', mockDelay]];
            const keyContext = {
                objectKey: 'bestObjEver',
            };

            hdmock.mockPUT(hdClient.options, keyContext, payloads);

            let called = false;
            hdClient.put(
                hdmock.streamString(content),
                hdmock.getPayloadLength(content),
                keyContext, '1',
                /* PUT is considered successful on timeout as we don't
                 * whether it really was stored or not
                 */
                (err, rawKey) => {
                    assert.ok(!called);
                    called = true;
                    assert.ifError(err);
                    assert.strictEqual(typeof rawKey, 'string');
                    const parts = hdclient.keyscheme.deserialize(rawKey);

                    const [endpoint, port] = hdClient.options
                              .endpoints[0].split(':');
                    assert.strictEqual(parts.nDataParts, 1);
                    assert.strictEqual(parts.nCodingParts, 0);
                    assert.strictEqual(parts.data[0].hostname, endpoint);
                    assert.strictEqual(parts.data[0].port, Number(port));
                    assert.ok(parts.data[0].key, keyContext.objectKey);
                    done();
                });
        });
    });
});

