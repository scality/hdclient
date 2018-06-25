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
            const [rawKey] =
                  hdmock.mockDELETE(hdClient.options, 'bestObjEver', [200]);
            hdClient.delete(rawKey, '1', err => {
                done(err);
            });
        });

        mocha.it('Not found key', function (done) {
            const hdClient = getDefaultClient();
            const [rawKey] =
                  hdmock.mockDELETE(hdClient.options, 'bestObjEver', [404]);
            hdClient.delete(rawKey, '1', err => {
                assert.strictEqual(err.infos.status, 404);
                done();
            });
        });

        mocha.it('Server error', function (done) {
            const hdClient = getDefaultClient();
            const [rawKey] =
                  hdmock.mockDELETE(hdClient.options, 'bestObjEver', [500]);
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
    });

    mocha.describe('GET', function () {
        mocha.it('Existing small key', function (done) {
            const hdClient = getDefaultClient();
            const payloads = [[200, 'Je suis une mite en pullover', 'data']];
            const [rawKey] =
                      hdmock.mockGET(hdClient.options, 'bestObjEver', payloads);

            hdClient.get(
                rawKey, undefined /* range */, '1',
                (err, httpReply) => {
                    assert.ifError(err);
                    assert.ok(httpReply);

                    // Sanity checks before buffering the stream
                    assert.strictEqual(httpReply.statusCode, payloads[0][0]);
                    assert.strictEqual(httpReply.headers['content-length'],
                                       payloads[0][1].length);

                    // Buffer the whole stream and perform checks on 'end' event
                    const readBufs = [];
                    httpReply.on('data', function (chunk) {
                        readBufs.push(chunk);
                    });
                    httpReply.on('end', function () {
                        const buf = readBufs.join('');
                        assert.strictEqual(buf.length, payloads[0][1].length);
                        assert.strictEqual(buf, payloads[0][1]);
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
            const payload = [200, content, 'data'];
            const [rawKey] = hdmock.mockGET(
                hdClient.options, 'bestObjEver', [payload]);

            hdClient.get(
                rawKey, undefined /* range */, '1',
                (err, httpReply) => {
                    assert.ifError(err);
                    assert.ok(httpReply);

                    // Sanity checks before buffering the stream
                    assert.strictEqual(httpReply.statusCode, payload[0]);
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

        mocha.it('Not found key', function (done) {
            const hdClient = getDefaultClient();
            const [rawKey] = hdmock.mockGET(
                hdClient.options, 'bestObjEver', [[404, '', 'data']]);

            hdClient.get(rawKey, null /* range */, '1', err => {
                assert.strictEqual(err.infos.status, 404);
                done();
            });
        });

        mocha.it('Server error', function (done) {
            const hdClient = getDefaultClient();
            const [rawKey] = hdmock.mockGET(
                hdClient.options, 'bestObjEver', [[500, '', 'data']]);

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
    });
});

