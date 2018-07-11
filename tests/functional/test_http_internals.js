'use strict'; // eslint-disable-line strict
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');
const nock = require('nock');

const hdclient = require('../../index');

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

mocha.describe('HTTP internals', function () {
    // Clean all HTTP mocks before starting the test
    mocha.beforeEach(nock.cleanAll);

    // Verify all mocks have been used - no garbage
    mocha.afterEach(function () {
        assert.ok(nock.isDone);
    });

    mocha.it('Socket error handling', function (done) {
        const hdClient = getDefaultClient();
        const [ip, port] = hdClient.options.policy.locations[0].split(':');
        const opts = hdclient.httpUtils.getCommonStoreRequestOptions(
            hdClient.httpAgent, ip, Number(port), 'test_key');
        opts.method = 'GET';
        opts.path = '/jesuisunemiteenpullover';
        const noLog = { error() {} };
        const expectedErrorMessage = 'something awful happened';

        nock(`http://${hdClient.options.policy.locations[0]}`)
            .get(opts.path)
            .replyWithError(expectedErrorMessage);

        const opContext = hdclient.httpUtils.makeOperationContext(
            { nDataParts: 1, nCodingParts: 0, nChunks: 1 });
        const reqContext = {
            opContext,
            chunkId: 0,
            fragmentId: 0,
        };

        hdclient.httpUtils.newRequest(
            opts, noLog, reqContext, 0, opCtx => {
                assert.strictEqual(opCtx.status[0].nError, 1);
                assert.ok(opCtx.status[0].statuses[0].error);
                const returnedError = opCtx.status[0].statuses[0].error;
                assert.strictEqual(expectedErrorMessage,
                                   returnedError.message);
                done();
            }).end();
    });

    mocha.it('Request timeout', function (done) {
        const hdClient = getDefaultClient();
        const [ip, port] = hdClient.options.policy.locations[0].split(':');
        const opts = hdclient.httpUtils.getCommonStoreRequestOptions(
            hdClient.httpAgent, ip, Number(port), 'test_key');
        opts.method = 'GET';
        opts.path = '/jesuisunemiteenpullover';
        const noLog = { error() {} };
        const expectedErrorMessage = 'Timeout';

        nock(`http://${hdClient.options.policy.locations[0]}`)
            .get(opts.path)
            .delay(hdClient.options.requestTimeoutMs + 10)
            .reply(200, 'je suis une mite en pull over');

        const opContext = hdclient.httpUtils.makeOperationContext(
            { nDataParts: 1, nCodingParts: 0, nChunks: 1 });
        const reqContext = {
            opContext,
            chunkId: 0,
            fragmentId: 0,
        };

        hdclient.httpUtils.newRequest(
            opts, noLog, reqContext,
            hdClient.options.requestTimeoutMs,
            opCtx => {
                assert.strictEqual(opCtx.status[0].nTimeout, 1);
                assert.ok(opCtx.status[0].statuses[0].error);
                const returnedError = opCtx.status[0].statuses[0].error;
                assert.strictEqual(expectedErrorMessage,
                                   returnedError.message);
                done();
            }).end();
    });
});

