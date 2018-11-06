'use strict'; // eslint-disable-line strict
/* eslint-disable max-len */
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');

const { hdclient, config } = require('../../index');

mocha.describe('Hyperdrive Client', function () {
    const create = function (opts) {
        return new hdclient.HyperdriveClient(opts);
    };

    const thrownErrorValidation = function (thrown, expected) {
        return thrown instanceof config.InvalidConfigError &&
            thrown.message === expected.message;
    };

    // ----------------------------------------------------------------------------
    mocha.describe('Data placement policy', function () {
        mocha.it('No data placement policy', function (done) {
            const args = {};
            const expectedError = new config.InvalidConfigError(
                'policy', 'undefined', 'Expected data placement policy');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('Bad type of cluster description', function (done) {
            const args = { policy: { cluster: 42 } };
            const expectedError = new config.InvalidConfigError(
                'policy.cluster', 42,
                'Expected a cluster topology object description');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('Empty cluster', function (done) {
            const args = {
                policy: {
                    cluster: {
                        name: 'DummyCluster',
                    },
                },
            };
            const expectedError = new config.InvalidConfigError(
                'policy.cluster.components', undefined,
                'A cluster expects at least 1 described component');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });


        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        mocha.describe('Validate cluster leaves - hyperdrive', function () {
            mocha.it('No name', function (done) {
                const args = {
                    policy: {
                        cluster: {
                            components: [{}], // Empty hyperdrive
                        },
                    },
                };
                const expectedError = new config.InvalidConfigError(
                    'hyperdrive.name', undefined,
                    'Deepest level require a unique name field (UUID)');
                assert.throws(() => create(args),
                              function (thrown) {
                                  return thrownErrorValidation(thrown, expectedError);
                              });
                done();
            });

            mocha.it('No weight', function (done) {
                const args = {
                    policy: {
                        cluster: {
                            components: [{ name: 'hd1' }],
                        },
                    },
                };
                const expectedError = new config.InvalidConfigError(
                    'component.staticWeight', undefined,
                    'Static weight must be a positive number');
                assert.throws(() => create(args),
                              function (thrown) {
                                  return thrownErrorValidation(thrown, expectedError);
                              });
                done();
            });

            mocha.it('Bad weight', function (done) {
                const args = {
                    policy: {
                        cluster: {
                            components: [{ name: 'hd1', staticWeight: -0.1 }],
                        },
                    },
                };
                const expectedError = new config.InvalidConfigError(
                    'component.staticWeight', -0.1,
                    'Static weight must be a positive number');
                assert.throws(() => create(args),
                              function (thrown) {
                                  return thrownErrorValidation(thrown, expectedError);
                              });
                done();
            });

            mocha.it('Bad ftype', function (done) {
                const args = {
                    policy: {
                        cluster: {
                            components: [{ name: 'hd1', ftype: 'fake' }],
                        },
                    },
                };
                const expectedError = new config.InvalidConfigError(
                    'hyperdrive.ftype', 'fake',
                    'ftype field expects either "data", "coding" or "both"');
                assert.throws(() => create(args),
                              function (thrown) {
                                  return thrownErrorValidation(thrown, expectedError);
                              });
                done();
            });

            mocha.it('Bad affinity', function (done) {
                const args = {
                    policy: {
                        cluster: {
                            components: [{ name: 'hd1', affinity: 'fake' }],
                        },
                    },
                };
                const expectedError = new config.InvalidConfigError(
                    'hyperdrive.affinity', 'fake',
                    'affinity expects either "soft" or "hard" as value');
                assert.throws(() => create(args),
                              function (thrown) {
                                  return thrownErrorValidation(thrown, expectedError);
                              });
                done();
            });

            mocha.it('Defaults', function (done) {
                const policy = {
                    cluster: {
                        components: [{ name: 'hd1', staticWeight: 0.1 }],
                    },
                };
                const valid = config.validatePolicySection(policy);
                if (!valid.configIsValid) {
                    done(valid.configError);
                    return;
                }

                assert.strictEqual(valid.config.cluster.components[0].affinity, 'hard');
                assert.strictEqual(valid.config.cluster.components[0].ftype, 'both');
                assert.strictEqual(valid.config.cluster.components[0].dynamicWeights.length, 1);
                assert.strictEqual(valid.config.cluster.components[0].dynamicWeights[0], 0.1);
                assert.strictEqual(valid.config.cluster.components[0].dynamicSum, 0.1);

                assert.strictEqual(valid.config.cluster.name, 'Internal-0-0');
                assert.strictEqual(valid.config.cluster.dynamicWeights.length, 1);
                assert.strictEqual(valid.config.cluster.dynamicWeights[0], 0.1);
                assert.strictEqual(valid.config.cluster.dynamicSum, 0.1);

                assert.strictEqual(valid.config.minSplitSize, 0);
                done();
            });
        });

        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        mocha.describe('Validate complex clusters', function () {
            mocha.it('Bad affinity container', function (done) {
                const args = {
                    policy: {
                        cluster: {
                            affinity: 'does not exist',
                            components: [],
                        },
                    },
                };
                const expectedError = new config.InvalidConfigError(
                    'component.affinity', 'does not exist',
                    'affinity expects either "soft" or "hard" as value');
                assert.throws(() => create(args),
                              function (thrown) {
                                  return thrownErrorValidation(thrown, expectedError);
                              });
                done();
            });

            mocha.it('Bad ftype container', function (done) {
                const args = {
                    policy: {
                        cluster: {
                            ftype: 'does not exist',
                            components: [],
                        },
                    },
                };
                const expectedError = new config.InvalidConfigError(
                    'component.ftype', 'does not exist',
                    'ftype field expects either "data", "coding" or "both"');
                assert.throws(() => create(args),
                              function (thrown) {
                                  return thrownErrorValidation(thrown, expectedError);
                              });
                done();
            });

            mocha.it('Invalid aggregated weight', function (done) {
                const args = {
                    policy: {
                        cluster: {
                            components: [
                                { name: 'hd1', staticWeight: 0 },
                                { name: 'hd2', staticWeight: 0 },
                            ],
                        },
                    },
                };
                const expectedError = new config.InvalidConfigError(
                    'Aggregated weights', 0,
                    'A container must have at least 1 sub-components with a non-zero staticWeight');
                assert.throws(() => create(args),
                              function (thrown) {
                                  return thrownErrorValidation(thrown, expectedError);
                              });
                done();
            });

            mocha.it('Flat - multiple hyperdrives', function (done) {
                const expectedWeights = [0, 10, 0.2, 3.14159];
                const policy = {
                    cluster: {
                        name: 'OneClusterToStoreThemAll',
                        affinity: 'hard', // Only 1 fragment allowed, not very useful
                        components: [
                            { name: 'hd1', staticWeight: expectedWeights[0], affinity: 'hard' },
                            { name: '42', staticWeight: expectedWeights[1], affinity: 'soft', ftype: 'coding' },
                            { name: '終わり', staticWeight: expectedWeights[2], ftype: 'both' },
                            { name: 'cuillère', staticWeight: expectedWeights[3], ftype: 'data' },
                        ],
                    },
                };
                const valid = config.validatePolicySection(policy);
                if (!valid.configIsValid) {
                    done(valid.configError);
                    return;
                }

                // Validate each hyperdrive
                assert.strictEqual(4, valid.config.cluster.components.length);
                valid.config.cluster.components.forEach((hyperdrive, i) => {
                    const expected = policy.cluster.components[i];
                    assert.strictEqual(hyperdrive.name, expected.name);
                    assert.strictEqual(hyperdrive.staticWeight, expected.staticWeight);
                    assert.strictEqual(hyperdrive.dynamicWeights.length, 1);
                    assert.strictEqual(hyperdrive.dynamicWeights[0], expected.staticWeight);
                    assert.strictEqual(hyperdrive.dynamicSum, expected.staticWeight);
                    assert.strictEqual(hyperdrive.affinity, expected.affinity || 'hard');
                    assert.strictEqual(hyperdrive.ftype, expected.ftype || 'both');
                });

                // Validate cluster level
                assert.strictEqual(valid.config.cluster.name, policy.cluster.name);
                assert.strictEqual(valid.config.cluster.dynamicWeights.length,
                                   expectedWeights.length);
                assert.ok(valid.config.cluster.dynamicWeights.every((w, i) => w === expectedWeights[i]));
                assert.strictEqual(valid.config.cluster.dynamicSum,
                                   expectedWeights.reduce((a, b) => a + b, 0));
                done();
            });

            mocha.it('Balanced nesting - multi-site, multi-hyperdrives', function (done) {
                /* 2 + 1 over 3 sites, pushing coding framgent always on same site */
                const policy = {
                    cluster: {
                        name: 'OneClusterToStoreThemAll',
                        components: [
                            { name: 'SiteA',
                              affinity: 'hard',
                              ftype: 'data',
                              components: [
                                  { name: 'hd-A-1', staticWeight: 1 },
                                  { name: 'hd-A-2', staticWeight: 1 },
                              ],
                            },
                            { affinity: 'hard',
                              ftype: 'data',
                              components: [
                                  { name: 'hd-B-1', staticWeight: 1 },
                                  { name: 'hd-B-2', staticWeight: 1 },
                              ],
                            },
                            { name: 'ParitySite',
                              affinity: 'hard',
                              ftype: 'coding',
                              components: [
                                  { name: 'hd-P-1', staticWeight: 1 },
                                  { name: 'hd-P-2', staticWeight: 1 },
                              ],
                            },
                        ],
                    },
                };
                const valid = config.validatePolicySection(policy);
                if (!valid.configIsValid) {
                    done(valid.configError);
                    return;
                }

                // Validate each site
                assert.strictEqual(valid.config.cluster.components.length, 3);
                policy.cluster.components.forEach((site, i) => {
                    const parsedSite = valid.config.cluster.components[i];
                      // Validate each hyperdrive
                    assert.strictEqual(2, parsedSite.components.length);
                    parsedSite.components.forEach((hyperdrive, j) => {
                        const expectedHyperdrive = policy.cluster.components[i].components[j];
                        assert.strictEqual(hyperdrive.name, expectedHyperdrive.name);
                        assert.strictEqual(hyperdrive.staticWeight, expectedHyperdrive.staticWeight);
                        assert.strictEqual(hyperdrive.dynamicWeights.length, 1);
                        assert.strictEqual(hyperdrive.dynamicWeights[0], expectedHyperdrive.staticWeight);
                        assert.strictEqual(hyperdrive.dynamicSum, expectedHyperdrive.staticWeight);
                        assert.strictEqual(hyperdrive.affinity, 'hard');
                        assert.strictEqual(hyperdrive.ftype, 'both');
                    });

                    assert.strictEqual(parsedSite.name, site.name || `Internal-1-${i}`);
                    assert.strictEqual(parsedSite.affinity, 'hard');
                    assert.strictEqual(parsedSite.ftype, i === 2 ? 'coding' : 'data');
                    assert.strictEqual(parsedSite.dynamicSum, 2);
                    assert.strictEqual(parsedSite.dynamicWeights.length, 2);
                    assert.ok(parsedSite.dynamicWeights.every(w => w === 1));
                });

                // Validate cluster level
                assert.strictEqual(valid.config.cluster.dynamicWeights.length, 3);
                assert.ok(valid.config.cluster.dynamicWeights.every(w => w === 2));
                assert.strictEqual(valid.config.cluster.dynamicSum, 6);
                done();
            });

            mocha.it('Heterogenous cluster', function (done) {
                /* Mixed weight and description depth */
                const policy = {
                    cluster: {
                        components: [
                            { ftype: 'data',
                              components: [
                                  { name: 'hd-A-1', staticWeight: 1 },
                                  { name: 'hd-A-2', staticWeight: 1 },
                              ],
                            },
                            { ftype: 'coding', name: 'hdalone', staticWeight: 10 },
                        ],
                    },
                };
                const valid = config.validatePolicySection(policy);
                if (!valid.configIsValid) {
                    done(valid.configError);
                    return;
                }

                const cluster = valid.config.cluster;
                const site = cluster.components[0];
                const hd = cluster.components[1];

                // Validate site
                assert.strictEqual(site.name, 'Internal-1-0');
                assert.strictEqual(site.ftype, 'data');
                assert.strictEqual(site.affinity, 'soft');
                assert.strictEqual(site.dynamicWeights.length, 2);
                assert.strictEqual(site.dynamicWeights[0], 1);
                assert.strictEqual(site.dynamicWeights[1], 1);
                assert.strictEqual(site.dynamicSum, 2);

                // Validate standalone hyperdrive
                assert.strictEqual(hd.name, 'hdalone');
                assert.strictEqual(hd.ftype, 'coding');
                assert.strictEqual(hd.staticWeight, 10);
                assert.strictEqual(hd.dynamicWeights.length, 1);
                assert.strictEqual(hd.dynamicWeights[0], 10);
                assert.strictEqual(hd.dynamicSum, 10);

                // Validate cluster
                assert.strictEqual(cluster.name, 'Internal-0-0');
                assert.strictEqual(cluster.dynamicWeights.length, 2);
                assert.strictEqual(cluster.dynamicWeights[0], 2);
                assert.strictEqual(cluster.dynamicWeights[1], 10);
                assert.strictEqual(cluster.dynamicSum, 10 + 2);
                done();
            });
        });
    });

    // ----------------------------------------------------------------------------
    mocha.describe('Codes section', function () {
        mocha.it('No codes', function (done) {
            const args = {
                policy: {
                    cluster: { components: [{ name: 'a', staticWeight: 1 }] },
                },
            };
            const expectedError = new config.InvalidConfigError(
                'codes', undefined,
                'Expected an array of { pattern, dataParts, codingParts }');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('Empty codes array', function (done) {
            const codes = [];
            const expectedError = new config.InvalidConfigError(
                'codes', [],
                'Expected at least one code pattern');
            const validation = config.validateCodeSection(codes);
            assert.ok(!validation.configIsValid);
            thrownErrorValidation(validation.configError, expectedError);
            done();
        });

        mocha.it('No code type', function (done) {
            const codes = [{ type: 'FAKE' }];
            const expectedError = new config.InvalidConfigError(
                'type', 'FAKE',
                'Unknown code type (code 0)');
            const validation = config.validateCodeSection(codes);
            assert.ok(!validation.configIsValid);
            thrownErrorValidation(validation.configError, expectedError);
            done();
        });

        mocha.it('No dataParts', function (done) {
            const codes = [{ type: 'CP' }];
            const expectedError = new config.InvalidConfigError(
                'dataParts', undefined,
                'Expected integer strictly larger than 0 (code 0)');
            const validation = config.validateCodeSection(codes);
            assert.ok(!validation.configIsValid);
            thrownErrorValidation(validation.configError, expectedError);
            done();
        });

        mocha.it('Bad dataParts type', function (done) {
            const codes = [{ type: 'CP', dataParts: 'whatever' }];
            const expectedError = new config.InvalidConfigError(
                'dataParts', 'whatever',
                'Expected integer strictly larger than 0 (code 0)');
            const validation = config.validateCodeSection(codes);
            assert.ok(!validation.configIsValid);
            thrownErrorValidation(validation.configError, expectedError);
            done();
        });

        mocha.it('Invalid dataParts value', function (done) {
            const codes = [{ type: 'CP', dataParts: 0 }];
            const expectedError = new config.InvalidConfigError(
                'dataParts', 0,
                'Expected integer strictly larger than 0 (code 0)');
            const validation = config.validateCodeSection(codes);
            assert.ok(!validation.configIsValid);
            thrownErrorValidation(validation.configError, expectedError);
            done();
        });

        mocha.it('No codingParts', function (done) {
            const codes = [{ type: 'CP', dataParts: 1 }];
            const expectedError = new config.InvalidConfigError(
                'codingParts', undefined,
                'Expected integer larger than 0 (code 0)');
            const validation = config.validateCodeSection(codes);
            assert.ok(!validation.configIsValid);
            thrownErrorValidation(validation.configError, expectedError);
            done();
        });

        mocha.it('Bad codingParts type', function (done) {
            const codes = [{ type: 'CP',
                                     dataParts: 1,
                                     codingParts: [],
                           }];
            const expectedError = new config.InvalidConfigError(
                'codingParts', [],
                'Expected integer larger than 0 (code 0)');
            const validation = config.validateCodeSection(codes);
            assert.ok(!validation.configIsValid);
            thrownErrorValidation(validation.configError, expectedError);
            done();
        });

        mocha.it('Invalid codingParts value', function (done) {
            const codes = [{ type: 'RS',
                                     dataParts: 1,
                                     codingParts: -1,
                                   }];
            const expectedError = new config.InvalidConfigError(
                'codingParts', -1,
                'Expected integer larger than 0 (code 0)');
            const validation = config.validateCodeSection(codes);
            assert.ok(!validation.configIsValid);
            thrownErrorValidation(validation.configError, expectedError);
            done();
        });

        mocha.it('Invalid CP + codingParts', function (done) {
            const codes = [{ type: 'CP', dataParts: 1, codingParts: 1 }];
            const expectedError = new config.InvalidConfigError(
                'codingParts', 1,
                'Code type CP expects 0 coding parts (code 0)');
            const validation = config.validateCodeSection(codes);
            assert.ok(!validation.configIsValid);
            thrownErrorValidation(validation.configError, expectedError);
            done();
        });

        mocha.it('Invalid code pattern', function (done) {
            const codes = [{ type: 'RS', dataParts: 1, codingParts: 1 }];
            const expectedError = new config.InvalidConfigError(
                'pattern', undefined,
                'Expected a bucket/object regex pattern (code 0)');
            const validation = config.validateCodeSection(codes);
            assert.ok(!validation.configIsValid);
            thrownErrorValidation(validation.configError, expectedError);
            done();
        });
    });

    // ----------------------------------------------------------------------------
    mocha.describe('Every other errors...', function () {
        mocha.it('No opts', function (done) {
            const expectedError = new config.InvalidConfigError(
                '', '', 'No options passed');
            assert.throws(create,
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('Invalid request timeout', function (done) {
            const args = { policy: { cluster: { components: [{ name: 'hd1', staticWeight: 0.1 }] } },
                           codes: [{ type: 'CP', dataParts: 1, codingParts: 0, pattern: '.*' }],
                           requestTimeoutMs: -1,
                         };
            const expectedError = new config.InvalidConfigError(
                'requestTimeoutMs', -1,
                'Expected a positive number');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });

        mocha.it('Invalid error agent options', function (done) {
            const args = { policy: { cluster: { components: [{ name: 'hd1', staticWeight: 0.1 }] } },
                           codes: [{ type: 'RS', dataParts: 2, codingParts: 1, pattern: '\\*[a-z]*' }],
                           requestTimeoutMs: 0,
                         };
            const expectedError = new config.InvalidConfigError(
                'errorAgent.kafkaBrokers', undefined,
                'Expected a CSV list of hostnames');
            assert.throws(() => create(args),
                          function (thrown) {
                              return thrownErrorValidation(thrown, expectedError);
                          });
            done();
        });
    });

    // ----------------------------------------------------------------------------
    mocha.describe('Valid full configurations', function () {
        mocha.it('Simple configuration', function (done) {
            const args = { policy: { cluster: { components: [{ name: 'hd1', staticWeight: 0.1 }] } },
                           codes: [{ type: 'RS', dataParts: 2, codingParts: 1, pattern: '.*' }],
                           requestTimeoutMs: 0,
                           errorAgent: { kafkaBrokers: 'fake-1:7777,fake-2:1234' },
                         };
            const client = new hdclient.HyperdriveClient(args);
            assert.ok(client);
            assert.strictEqual(client.clientType, 'scality');
            const { configIsValid, configError } = config.validate(client.options);
            assert.ok(configIsValid);
            assert.ok(configError === null);
            done();
        });
    });
});
