'use strict'; // eslint-disable-line strict
/* eslint-disable max-len */
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');
const nock = require('nock');

const hdclient = require('../../index');
const hdmock = require('../utils');

mocha.describe('HTTP internals', function () {
    // Clean all HTTP mocks before starting the test
    mocha.beforeEach(nock.cleanAll);

    // Verify all mocks have been used - no garbage
    mocha.afterEach(function () {
        assert.ok(nock.isDone);
    });

    mocha.it('Query strings', function (done) {
        const hdClient = hdmock.getDefaultClient();
        const uuid = hdClient.conf.policy.cluster.components[0].name;
        const [ip, port] = hdClient.uuidmapping[uuid].split(':');

        const queries = { invisible: null, test: false, regular: 1 };
        const queryString = hdclient.httpUtils.makeQueryString(queries);
        assert.ok(queryString === 'test=false&regular=1'
                 || queryString === 'regular=1&test=false');

        const noQS = hdclient.httpUtils.getCommonStoreRequestOptions(
            hdClient.httpAgent, ip, Number(port), 'test_key');
        assert.strictEqual(noQS.path,
                           `${hdclient.protocol.specs.STORAGE_BASE_URL}/test_key`);

        const withQS = hdclient.httpUtils.getCommonStoreRequestOptions(
            hdClient.httpAgent, ip, Number(port), 'test_key', queryString);
        assert.strictEqual(withQS.path,
                           `${hdclient.protocol.specs.STORAGE_BASE_URL}/test_key?${queryString}`);
        done();
    });

    mocha.it('Socket error handling', function (done) {
        const hdClient = hdmock.getDefaultClient();
        const uuid = hdClient.conf.policy.cluster.components[0].name;
        const [ip, port] = hdClient.uuidmapping[uuid].split(':');
        const opts = hdclient.httpUtils.getCommonStoreRequestOptions(
            hdClient.httpAgent, ip, Number(port), 'test_key');
        opts.method = 'GET';
        opts.path = '/jesuisunemiteenpullover';
        const noLog = { error() {} };
        const expectedErrorDescription = 'something awful happened';

        nock(`http://${ip}:${port}`)
            .get(opts.path)
            .replyWithError(expectedErrorDescription);

        const opContext = hdclient.httpUtils.makeOperationContext(
            { nDataParts: 1, nCodingParts: 0, nChunks: 1 });
        const reqContext = {
            opContext,
            chunkId: 0,
            fragmentId: 0,
        };

        hdclient.httpUtils.newRequest(
            opts, noLog, reqContext, 0, reqCtx => {
                assert.strictEqual(reqCtx.opContext.status[0].nError, 1);
                assert.ok(reqCtx.opContext.status[0].statuses[0].error);
                const returnedError = reqCtx.opContext.status[0].
                          statuses[0].error;
                assert.strictEqual(returnedError.message, 'GETError');
                assert.strictEqual(returnedError.code, 500);
                assert.strictEqual(expectedErrorDescription,
                                   returnedError.description);
                done();
            }).end();
    });

    mocha.it('Request timeout', function (done) {
        const hdClient = hdmock.getDefaultClient();
        const uuid = hdClient.conf.policy.cluster.components[0].name;
        const [ip, port] = hdClient.uuidmapping[uuid].split(':');
        const opts = hdclient.httpUtils.getCommonStoreRequestOptions(
            hdClient.httpAgent, ip, Number(port), 'test_key');
        opts.method = 'GET';
        opts.path = '/jesuisunemiteenpullover';
        const noLog = { error() {} };

        nock(`http://${ip}:${port}`)
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
            reqCtx => {
                assert.strictEqual(reqCtx.opContext.status[0].nTimeout, 1);
                assert.ok(reqCtx.opContext.status[0].statuses[0].error);
                const returnedError = reqCtx.opContext.status[0]
                          .statuses[0].error;
                assert.strictEqual(returnedError.message, 'TimeoutError');
                assert.strictEqual(returnedError.code, 504);
                assert.strictEqual(
                    returnedError.description,
                    `No reply received in ${hdClient.options.requestTimeoutMs}ms`);
                done();
            }).end();
    });

    mocha.it('Request abort', function (done) {
        const hdClient = hdmock.getDefaultClient();
        const uuid = hdClient.conf.policy.cluster.components[0].name;
        const [ip, port] = hdClient.uuidmapping[uuid].split(':');
        const opts = hdclient.httpUtils.getCommonStoreRequestOptions(
            hdClient.httpAgent, ip, Number(port), 'test_key');
        opts.method = 'GET';
        opts.path = '/jesuisunemiteenpullover';
        const noLog = { error() {} };

        nock(`http://${ip}:${port}`)
            .get(opts.path)
            .delay(hdClient.options.requestTimeoutMs - 1)
            .reply(200, 'je suis une mite en pull over');

        const opContext = hdclient.httpUtils.makeOperationContext(
            { nDataParts: 1, nCodingParts: 0, nChunks: 1 });
        const reqContext = {
            opContext,
            chunkId: 0,
            fragmentId: 0,
        };

        const request = hdclient.httpUtils.newRequest(
            opts, noLog, reqContext,
            hdClient.options.requestTimeoutMs,
            reqCtx => {
                assert.strictEqual(reqCtx.opContext.status[0].nTimeout, 0);
                assert.strictEqual(reqCtx.opContext.status[0].nError, 1);
                assert.strictEqual(reqCtx.opContext.status[0].nOk, 0);
                assert.ok(reqCtx.opContext.status[0].statuses[0].error);
                const returnedError = reqCtx.opContext.status[0].
                          statuses[0].error;
                assert.strictEqual(returnedError.message, 'GETError');
                assert.strictEqual(returnedError.code, 500);
                assert.strictEqual(returnedError.description, 'socket hang up');
                done();
            });

        request.abort();
    });
});

