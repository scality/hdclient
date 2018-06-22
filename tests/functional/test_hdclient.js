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
});

