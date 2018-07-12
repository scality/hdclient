'use strict'; // eslint-disable-line strict
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');
const nock = require('nock');

const hdclient = require('../../index');
const hdmock = require('../utils');

const BadKeyError = hdclient.keyscheme.KeySchemeDeserializeError;

function strictCompareTopicContent(realContent, expectedContent) {
    if (realContent === undefined || expectedContent === undefined) {
        assert.strictEqual(realContent, expectedContent);
        return;
    }

    assert.strictEqual(realContent.length,
                       expectedContent.length);
    realContent.forEach((realLog, i) => {
        const expectedLog = expectedContent[i];
        assert.strictEqual(realLog.rawKey, expectedLog.rawKey);
        assert.strictEqual(realLog.toDelete.length,
                           expectedLog.toDelete.length);
        realLog.toDelete.forEach((realDeleted, j) => {
            const expectedDeleted = expectedLog.toDelete[j];
            assert.strictEqual(realDeleted[0], expectedDeleted[0]);
            assert.strictEqual(realDeleted[0], expectedDeleted[0]);
        });
    });
}

mocha.describe('DELETE', function () {
    // Clean all HTTP mocks before starting the test
    mocha.beforeEach(nock.cleanAll);

    // Verify all mocks have been used - no garbage
    mocha.afterEach(function () { assert.ok(nock.isDone); });

    const deleteTopic = hdclient.httpUtils.topics.delete;

    mocha.describe('Single hyperdrive', function () {
        mocha.it('Existing key', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const mocks = [
                { statusCode: 200 },
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient.options, 'bestObjEver', mocks);
            hdClient.delete(rawKey, '1', err => {
                const topic = hdmock.getTopic(hdClient, deleteTopic);
                strictCompareTopicContent(topic, undefined);
                done(err);
            });
        });

        mocha.it('Not found key', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const mocks = [
                { statusCode: 404 },
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient.options, 'bestObjEver', mocks);
            hdClient.delete(rawKey, '1', err => {
                const topic = hdmock.getTopic(hdClient, deleteTopic);
                strictCompareTopicContent(topic, undefined);
                done(err);
            });
        });

        mocha.it('Server error', function (done) {
            const hdClient = hdmock.getDefaultClient();
            const mocks = [
                { statusCode: 500 },
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient.options, 'bestObjEver', mocks);
            const expectedLoggedErrors = [{
                rawKey,
                toDelete: [[0, 0]],
            }];

            hdClient.delete(rawKey, '1', err => {
                assert.strictEqual(err.infos.status, mocks[0].statusCode);
                assert.strictEqual(err.infos.method, 'DELETE');
                const topic = hdmock.getTopic(hdClient, deleteTopic);
                strictCompareTopicContent(topic, expectedLoggedErrors);
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
            const mocks = [
                {
                    statusCode: 200,
                    timeoutMs: hdClient.options.requestTimeoutMs + 10,
                },
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient.options, 'bestObjEver', mocks);
            const expectedLoggedErrors = [{
                rawKey,
                toDelete: [[0, 0]],
            }];

            hdClient.delete(rawKey, '1', err => {
                assert.ok(err);
                assert.strictEqual(err.infos.status, 500);
                assert.strictEqual(err.infos.method, 'DELETE');
                const topic = hdmock.getTopic(hdClient, deleteTopic);
                strictCompareTopicContent(topic, expectedLoggedErrors);
                done();
            });
        });
    });

    mocha.describe('Multiple hyperdrives', function () {
        mocha.describe('Replication', function () {
            mocha.it('All success', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 3,
                    code: 'CP',
                    nData: 3,
                    nCoding: 0,
                });
                const mocks = [
                    { statusCode: 200 },
                    { statusCode: 200 },
                    { statusCode: 200 },
                ];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient.options, 'bestObjEver', mocks);
                hdClient.delete(rawKey, '1', err => {
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    strictCompareTopicContent(topic, undefined);
                    done(err);
                });
            });

            mocha.it('404 on 1 part', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 3,
                    code: 'CP',
                    nData: 3,
                    nCoding: 0,
                });
                const mocks = [
                    { statusCode: 200 },
                    { statusCode: 404 },
                    { statusCode: 200 },
                ];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient.options, 'bestObjEver', mocks);
                hdClient.delete(rawKey, '1', err => {
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    strictCompareTopicContent(topic, undefined);
                    done(err);
                });
            });

            mocha.it('Error on 1 part', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 3,
                    code: 'CP',
                    nData: 3,
                    nCoding: 0,
                });
                const mocks = [
                    { statusCode: 200 },
                    { statusCode: 200 },
                    { statusCode: 500 },
                ];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient.options, 'bestObjEver', mocks);
                const expectedLogged = [{
                    rawKey,
                    toDelete: [[0, 2]],
                }];

                hdClient.delete(rawKey, '1', err => {
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    strictCompareTopicContent(topic, expectedLogged);
                    done(err);
                });
            });

            mocha.it('Errors on all parts', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 3,
                    code: 'CP',
                    nData: 3,
                    nCoding: 0,
                });
                const mocks = [
                    { statusCode: 503 },
                    { statusCode: 403 },
                    { statusCode: 500 },
                ];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient.options, 'bestObjEver', mocks);
                const expectedLogged = [{
                    rawKey,
                    toDelete: [[0, 0], [0, 1], [0, 2]],
                }];

                hdClient.delete(rawKey, '1', err => {
                    assert.ok(err);
                    assert.strictEqual(err.infos.status, 503);
                    assert.strictEqual(err.infos.method, 'DELETE');
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    strictCompareTopicContent(topic, expectedLogged);
                    done();
                });
            });
        });

        mocha.describe('Erasure coding', function () {
            mocha.it('All success', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 3,
                    code: 'RS',
                    nData: 2,
                    nCoding: 1,
                });
                const mocks = [
                    { statusCode: 200 },
                    { statusCode: 200 },
                    { statusCode: 200 },
                ];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient.options, 'bestObjEver', mocks);
                hdClient.delete(rawKey, '1', err => {
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    strictCompareTopicContent(topic, undefined);
                    done(err);
                });
            });

            mocha.it('404 on 1 part', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 3,
                    code: 'RS',
                    nData: 2,
                    nCoding: 1,
                });
                const mocks = [
                    { statusCode: 404 },
                    { statusCode: 200 },
                    { statusCode: 200 },
                ];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient.options, 'bestObjEver', mocks);
                hdClient.delete(rawKey, '1', err => {
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    strictCompareTopicContent(topic, undefined);
                    done(err);
                });
            });

            mocha.it('Error on 1 part', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 3,
                    code: 'RS',
                    nData: 2,
                    nCoding: 1,
                });
                const mocks = [
                    { statusCode: 200 },
                    { statusCode: 500 },
                    { statusCode: 404 },
                ];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient.options, 'bestObjEver', mocks);
                const expectedLogged = [{
                    rawKey,
                    toDelete: [[0, 1]],
                }];

                hdClient.delete(rawKey, '1', err => {
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    strictCompareTopicContent(topic, expectedLogged);
                    done(err);
                });
            });

            mocha.it('Errors on all parts', function (done) {
                const hdClient = hdmock.getDefaultClient({
                    nLocations: 3,
                    code: 'RS',
                    nData: 2,
                    nCoding: 1,
                });
                const mocks = [
                    { statusCode: 500 },
                    { statusCode: 400 },
                    { statusCode: 500 },
                ];
                const { rawKey } = hdmock.mockDELETE(
                    hdClient.options, 'bestObjEver', mocks);
                const expectedLogged = [{
                    rawKey,
                    toDelete: [[0, 0], [0, 1], [0, 2]],
                }];

                hdClient.delete(rawKey, '1', err => {
                    assert.ok(err);
                    assert.strictEqual(err.infos.status, 500);
                    assert.strictEqual(err.infos.method, 'DELETE');
                    const topic = hdmock.getTopic(hdClient, deleteTopic);
                    strictCompareTopicContent(topic, expectedLogged);
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
            const hdClient = hdmock.getDefaultClient({
                nLocations: 3,
                code: 'RS',
                nData: 2,
                nCoding: 1,
            });
            const mocks = [
                { statusCode: 200 },
                { statusCode: 500 },
                { statusCode: 404 },
            ];
            const { rawKey } = hdmock.mockDELETE(
                hdClient.options, 'bestObjEver', mocks);

            hdClient.errorAgent.nextError = new Error('Failed to queue');
            const expectedLogged = [{
                rawKey,
                toDelete: [[0, 1]],
            }];

            hdClient.delete(rawKey, '1', err => {
                assert.ok(err);
                assert.strictEqual(err.infos.status, 500);
                assert.strictEqual(err.message, 'Failed to queue');
                const topic = hdmock.getTopic(hdClient, deleteTopic);
                strictCompareTopicContent(topic, expectedLogged);
                done();
            });
        });
    });
});
