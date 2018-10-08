'use strict'; // eslint-disable-line strict
/* eslint-disable max-len */
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');
const nock = require('nock');

const hdclient = require('../../index');
const hdmock = require('../utils');

mocha.describe('DELETE', function () {
    // Clean all HTTP mocks before starting the test
    mocha.beforeEach(nock.cleanAll);

    // Verify all mocks have been used - no garbage
    mocha.afterEach(function () { assert.ok(nock.isDone); });

    const deleteTopic = hdclient.httpUtils.topics.delete;
    const keyContext = {
        bucketName: 'testbucket',
        objectKey: 'best / Obj~Ever!',
        version: 1,
    };

    mocha.describe('Single hyperdrive', function () {
        mocha.it('Existing key', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const mocks = [
                [{ statusCode: 200 }],
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient, keyContext, 1024, mocks);
            hdClient.delete(rawKey, '1', err => {
                const topic = hdmock.getTopic(hdClient, deleteTopic);
                hdmock.strictCompareTopicContent(
                    topic, undefined);
                done(err);
            });
        });

        mocha.it('Not found key', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const mocks = [
                [{ statusCode: 404 }],
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient, keyContext, 1024, mocks);
            hdClient.delete(rawKey, '1', err => {
                const topic = hdmock.getTopic(hdClient, deleteTopic);
                hdmock.strictCompareTopicContent(
                    topic, undefined);
                done(err);
            });
        });

        mocha.it('Server error', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const mocks = [
                [{ statusCode: 500 }],
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient, keyContext, 1024, mocks);
            const expectedLoggedErrors = [{
                rawKey,
                fragments: [[0, 0]],
                version: 1,
            }];

            hdClient.delete(rawKey, '1', err => {
                assert.strictEqual(err.code, mocks[0][0].statusCode);
                assert.strictEqual(err.infos.method, 'DELETE');
                const topic = hdmock.getTopic(hdClient, deleteTopic);
                hdmock.strictCompareTopicContent(
                    topic, expectedLoggedErrors);
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
            const mocks = [
                [{
                    statusCode: 200,
                    timeoutMs: hdClient.options.requestTimeoutMs + 10,
                }],
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient, keyContext, 1024, mocks);
            const expectedLoggedErrors = [{
                rawKey,
                fragments: [[0, 0]],
                version: 1,
            }];

            hdClient.delete(rawKey, '1', err => {
                assert.ok(err);
                assert.strictEqual(err.code, 504);
                const topic = hdmock.getTopic(hdClient, deleteTopic);
                hdmock.strictCompareTopicContent(
                    topic, expectedLoggedErrors);
                done();
            });
        });
    });

    mocha.describe('Multiple hyperdrives', function () {
        mocha.describe('Replication', function () {
            mocha.it('All success', function (done) {
                const codes = [{ type: 'CP', dataParts: 3, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes });
                const mocks = [[
                    { statusCode: 200 },
                    { statusCode: 200 },
                    { statusCode: 200 },
                ]];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient, keyContext, 1024, mocks);
                hdClient.delete(rawKey, '1', err => {
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);
                    done(err);
                });
            });

            mocha.it('404 on 1 part', function (done) {
                const codes = [{ type: 'CP', dataParts: 3, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes });
                const mocks = [[
                    { statusCode: 200 },
                    { statusCode: 404 },
                    { statusCode: 200 },
                ]];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient, keyContext, 1024, mocks);
                hdClient.delete(rawKey, '1', err => {
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);
                    done(err);
                });
            });

            mocha.it('Error on 1 part', function (done) {
                const codes = [{ type: 'CP', dataParts: 3, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes });
                const mocks = [[
                    { statusCode: 200 },
                    { statusCode: 200 },
                    { statusCode: 500 },
                ]];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient, keyContext, 1024, mocks);
                const expectedLogged = [{
                    rawKey,
                    fragments: [[0, 2]],
                    version: 1,
                }];

                hdClient.delete(rawKey, '1', err => {
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        topic, expectedLogged);
                    done(err);
                });
            });

            mocha.it('Errors on all parts', function (done) {
                const codes = [{ type: 'CP', dataParts: 3, codingParts: 0, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes });
                const mocks = [[
                    { statusCode: 503 },
                    { statusCode: 403 },
                    { statusCode: 500 },
                ]];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient, keyContext, 1024, mocks);
                const expectedLogged = [{
                    rawKey,
                    fragments: [[0, 0], [0, 1], [0, 2]],
                    version: 1,
                }];

                hdClient.delete(rawKey, '1', err => {
                    assert.ok(err);
                    assert.strictEqual(err.code, 503);
                    assert.strictEqual(err.infos.method, 'DELETE');
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        topic, expectedLogged);
                    done();
                });
            });
        });

        mocha.describe('Erasure coding', function () {
            mocha.it('All success', function (done) {
                const codes = [{ type: 'RS', dataParts: 2, codingParts: 1, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes });
                const mocks = [[
                    { statusCode: 200 },
                    { statusCode: 200 },
                    { statusCode: 200 },
                ]];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient, keyContext, 1024, mocks);
                hdClient.delete(rawKey, '1', err => {
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);
                    done(err);
                });
            });

            mocha.it('404 on 1 part', function (done) {
                const codes = [{ type: 'RS', dataParts: 2, codingParts: 1, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes });
                const mocks = [[
                    { statusCode: 404 },
                    { statusCode: 200 },
                    { statusCode: 200 },
                ]];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient, keyContext, 1024, mocks);
                hdClient.delete(rawKey, '1', err => {
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        topic, undefined);
                    done(err);
                });
            });

            mocha.it('Error on 1 part', function (done) {
                const codes = [{ type: 'RS', dataParts: 2, codingParts: 1, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes });
                const mocks = [[
                    { statusCode: 200 },
                    { statusCode: 500 },
                    { statusCode: 404 },
                ]];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient, keyContext, 1024, mocks);
                const expectedLogged = [{
                    rawKey,
                    fragments: [[0, 1]],
                    version: 1,
                }];

                hdClient.delete(rawKey, '1', err => {
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        topic, expectedLogged);
                    done(err);
                });
            });

            mocha.it('Errors on all parts', function (done) {
                const codes = [{ type: 'RS', dataParts: 2, codingParts: 1, pattern: '.*' }];
                const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes });
                const mocks = [[
                    { statusCode: 500 },
                    { statusCode: 400 },
                    { statusCode: 500 },
                ]];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient, keyContext, 1024, mocks);
                const expectedLogged = [{
                    rawKey,
                    fragments: [[0, 0], [0, 1], [0, 2]],
                    version: 1,
                }];

                hdClient.delete(rawKey, '1', err => {
                    assert.ok(err);
                    assert.strictEqual(err.code, 500);
                    assert.strictEqual(err.infos.method, 'DELETE');
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    hdmock.strictCompareTopicContent(
                        topic, expectedLogged);
                    done();
                });
            });
        });
    });

    mocha.describe('Persisting error edge cases', function () {
        mocha.it('Failed to persit', function (done) {
            /* Same exact scenario as Erasure Coding 'Error on 1 part'
             * but we failed to persist orphans, expecting resulting error
             */
            const codes = [{ type: 'RS', dataParts: 2, codingParts: 1, pattern: '.*' }];
            const hdClient = hdmock.getDefaultClient({ nLocations: 3, codes });
            const mocks = [[
                { statusCode: 200 },
                { statusCode: 500 },
                { statusCode: 404 },
            ]];
            const { rawKey } = hdmock.mockDELETE(
                hdClient, keyContext, 1024, mocks);
            hdClient.errorAgent.nextError = new Error('Broken by Design');

            hdClient.delete(rawKey, '1', err => {
                assert.ok(err);
                assert.strictEqual(err.message, 'InternalError');
                assert.strictEqual(err.code, 500);
                assert.strictEqual(
                    err.description,
                    'Failed to persist orphaned fragments: Broken by Design');
                const topic = hdmock.getTopic(hdClient, deleteTopic);
                hdmock.strictCompareTopicContent(
                    topic, undefined);
                done();
            });
        });
    });

    mocha.describe('Split', function () {
        mocha.it('Existing key', function (done) {
            const size = 15000;
            const hdClient = hdmock.getDefaultClient(
                { minSplitSize: size });
            const mocks = [
                [{ statusCode: 200 }],
                [{ statusCode: 200 }],
                [{ statusCode: 200 }],
                [{ statusCode: 200 }],
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient, keyContext, 4 * size, mocks);
            hdClient.delete(rawKey, '1', err => {
                const topic = hdmock.getTopic(hdClient, deleteTopic);
                hdmock.strictCompareTopicContent(
                    topic, undefined);
                done(err);
            });
        });

        mocha.it('Not found key', function (done) {
            const size = 8192;
            const hdClient = hdmock.getDefaultClient(
                { minSplitSize: size });
            const mocks = [
                [{ statusCode: 404 }],
                [{ statusCode: 200 }],
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient, keyContext, 2 * size, mocks);
            hdClient.delete(rawKey, '1', err => {
                const topic = hdmock.getTopic(hdClient, deleteTopic);
                hdmock.strictCompareTopicContent(
                    topic, undefined);
                done(err);
            });
        });

        mocha.it('Server error', function (done) {
            const size = 15000;
            const hdClient = hdmock.getDefaultClient(
                { minSplitSize: size });
            const mocks = [
                [{ statusCode: 200 }],
                [{ statusCode: 500 }],
                [{ statusCode: 404 }],
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient, keyContext, 3 * size, mocks);
            const expectedLoggedErrors = [{
                rawKey,
                fragments: [[1, 0]],
                version: 1,
            }];

            hdClient.delete(rawKey, '1', err => {
                const topic = hdmock.getTopic(hdClient, deleteTopic);
                hdmock.strictCompareTopicContent(
                    topic, expectedLoggedErrors);
                done(err);
            });
        });


        mocha.it('Timeout', function (done) {
            const size = 1024;
            const hdClient = hdmock.getDefaultClient(
                { minSplitSize: size });
            const mocks = [
                [{ statusCode: 200 }],
                [{
                    statusCode: 200,
                    timeoutMs: hdClient.options.requestTimeoutMs + 10,
                }],
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient, keyContext, 6000, mocks);
            const expectedLoggedErrors = [{
                rawKey,
                fragments: [[1, 0]],
                version: 1,
            }];

            hdClient.delete(rawKey, '1', err => {
                const topic = hdmock.getTopic(hdClient, deleteTopic);
                hdmock.strictCompareTopicContent(
                    topic, expectedLoggedErrors);
                done(err);
            });
        });
    });

    mocha.describe('Split + erasure coding', function () {
        mocha.it('All in one', function (done) {
            const size = 15000;
            const codes = [{ type: 'RS', dataParts: 2, codingParts: 1, pattern: '.*' }];
            const hdClient = hdmock.getDefaultClient({
                nLocations: 3,
                codes,
                minSplitSize: size });
            const mocks = [
                [{ statusCode: 200 }, { statusCode: 200 }, { statusCode: 404 }],
                [{ statusCode: 200 }, { statusCode: 500 }, { statusCode: 200 }],
                [{ statusCode: 500 }, { statusCode: 200 }, { statusCode: 503 }],
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient, keyContext, 3 * size, mocks);
            const expectedLoggedErrors = [{
                rawKey,
                fragments: [[1, 1], [2, 0], [2, 2]],
                version: 1,
            }];

            hdClient.delete(rawKey, '1', err => {
                const topic = hdmock.getTopic(hdClient, deleteTopic);
                hdmock.strictCompareTopicContent(
                    topic, expectedLoggedErrors);
                done(err);
            });
        });
    });
});
