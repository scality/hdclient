'use strict'; // eslint-disable-line strict
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');
const nock = require('nock');

const hdclient = require('../../index');
const hdmock = require('../utils');

const BadKeyError = hdclient.keyscheme.KeySchemeDeserializeError;

function getDefaultClient() {
    const conf = {
        policy: { locations: ['hyperdrive-store1:8888'] },
        dataParts: 1,
        codingParts: 0,
        requestTimeoutMs: 10,
    };

    const client = new hdclient.client.HyperdriveClient(conf);
    client.logging.config.update({ level: 'fatal', dump: 'fatal' });
    return client;
}

mocha.describe('DELETE', function () {
    // Clean all HTTP mocks before starting the test
    mocha.beforeEach(nock.cleanAll);

    // Verify all mocks have been used - no garbage
    mocha.afterEach(function () { assert.ok(nock.isDone); });

    mocha.describe('Single hyperdrive', function () {
        mocha.it('Existing key', function (done) {
            const hdClient = getDefaultClient();
            const mocks = [
                { statusCode: 200 },
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient.options, 'bestObjEver', mocks);
            hdClient.delete(rawKey, '1', err => {
                done(err);
            });
        });

        mocha.it('Not found key', function (done) {
            const hdClient = getDefaultClient();
            const mocks = [
                { statusCode: 404 },
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient.options, 'bestObjEver', mocks);
            hdClient.delete(rawKey, '1', err => {
                assert.strictEqual(err.infos.status, mocks[0].statusCode);
                done();
            });
        });

        mocha.it('Server error', function (done) {
            const hdClient = getDefaultClient();
            const mocks = [
                { statusCode: 500 },
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient.options, 'bestObjEver', mocks);
            hdClient.delete(rawKey, '1', err => {
                assert.strictEqual(err.infos.status, mocks[0].statusCode);
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
            const mocks = [
                {
                    statusCode: 200,
                    timeoutMs: hdClient.options.requestTimeoutMs + 10,
                },
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient.options, 'bestObjEver', mocks);

            hdClient.delete(rawKey, '1', err => {
                assert.ok(err);
                done();
            });
        });
    });
});
