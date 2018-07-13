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
                    assert.ifError(err);
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
                    done();
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
                    assert.ifError(err);
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
                    done();
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
                /* PUT is considered successful on timeout as we don't
                 * whether it really was stored or not
                 */
                (err, rawKey) => {
                    assert.ok(!called);
                    called = true;
                    assert.ok(err);
                    done();
                });
        });
    });
});
