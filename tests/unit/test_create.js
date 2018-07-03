'use strict'; // eslint-disable-line strict
/* eslint-disable max-len */
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');

const { client, config } = require('../../index');

mocha.describe('Hyperdrive Client', function () {
    mocha.describe('Configuration tests', function () {
        const create = function (opts) {
            return new client.HyperdriveClient(opts);
        };

        const thrownErrorValidation = function (thrown, expected) {
            return thrown instanceof config.InvalidConfigError &&
                thrown.message === expected.message;
        };

        mocha.it('No opts', function (done) {
            const expectedError = new config.InvalidConfigError('', '', 'No options passed');
            assert.throws(create,
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('No data placement policy', function (done) {
            const args = {};
            const expectedError = new config.InvalidConfigError('policy', 'undefined', 'Expected data placement policy');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('Bad locations type', function (done) {
            const args = { policy: { locations: 42 } };
            const expectedError = new config.InvalidConfigError('policy.locations', 42,
                                                                'Expected an array of string');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('Bad enpoint type', function (done) {
            const args = { policy: { locations: ['localhost:8080', 42] } };
            const expectedError = new config.InvalidConfigError('policy.locations', ['localhost:8080', 42],
                                                                'Expected an array of string');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('No enpoint', function (done) {
            const args = { policy: { locations: [] } };
            const expectedError = new config.InvalidConfigError('policy.locations', [],
                                                                'Expected at least 1 endpoint');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('No dataParts', function (done) {
            const args = { policy: { locations: ['localhost:8080'] } };
            const expectedError = new config.InvalidConfigError('dataParts', undefined,
                                                                'Expected integer larger than 1');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('Bad dataParts type', function (done) {
            const args = { policy: { locations: ['localhost:8080'] },
                           dataParts: 'whatever',
                         };
            const expectedError = new config.InvalidConfigError('dataParts', 'whatever',
                                                                'Expected integer larger than 1');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('Invalid dataParts value', function (done) {
            const args = { policy: { locations: ['localhost:8080'] },
                           dataParts: 0,
                         };
            const expectedError = new config.InvalidConfigError('dataParts', 0,
                                                                'Expected integer larger than 1');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('No codingParts', function (done) {
            const args = { policy: { locations: ['localhost:8080'] },
                           dataParts: 1,
                         };
            const expectedError = new config.InvalidConfigError('codingParts', undefined,
                                                                'Expected integer larger than 0');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('Bad codingParts type', function (done) {
            const args = { policy: { locations: ['localhost:8080'] },
                           dataParts: 1,
                           codingParts: [],
                         };
            const expectedError = new config.InvalidConfigError('codingParts', [],
                                                                'Expected integer larger than 0');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('Invalid codingParts value', function (done) {
            const args = { policy: { locations: ['localhost:8080'] },
                           dataParts: 1,
                           codingParts: -1,
                         };
            const expectedError = new config.InvalidConfigError('codingParts', -1,
                                                                'Expected integer larger than 0');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('Not enough endpoints', function (done) {
            const args = { policy: { locations: ['localhost:8080'] },
                           dataParts: 1,
                           codingParts: 1,
                         };
            const expectedError = new config.InvalidConfigError('totalParts', 2,
                                                                'Expected less parts than data locations');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('Valid configuration', function (done) {
            const args = { policy: { locations: ['server1', 'server2', 'server3'] },
                           dataParts: 2,
                           codingParts: 1,
                           requestTimeoutMs: 0,
                         };
            const hdclient = new client.HyperdriveClient(args);
            assert.ok(hdclient);
            assert.strictEqual(hdclient.clientType, 'scality');
            const [confIsOk, err] = config.validate(hdclient.options);
            assert.ok(confIsOk);
            assert.ok(err === null);
            done();
        });
    });
});
