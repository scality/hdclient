'use strict'; // eslint-disable-line strict
/* eslint-disable max-len */
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');

const { placement } = require('../../index');

mocha.describe('Data placement', function () {
    mocha.it('MatchingTypeSampling', function (done) {
        const component = {
            components: [
                { ftype: 'data' },
                { ftype: 'data' },
                { ftype: 'coding' },
                { ftype: 'both' },
            ],
        };
        const idxEntry = {
            weights: [0.25, 0.25, 0.25, 0.25],
            sum: 1.0,
        };

        /* Sample data location */
        for (let i = 0; i < 64; i++) {
            const sample = placement.sampleMatchingTypeComponent(
                component, idxEntry, 'data');
            assert.ok(sample === 0 || sample === 1 || sample === 3);
        }

        /* Sample coding location */
        for (let i = 0; i < 64; i++) {
            const sample = placement.sampleMatchingTypeComponent(
                component, idxEntry, 'coding');
            assert.ok(sample === 2 || sample === 3);
        }

        /* Restrict coding placement */
        component.components[2].ftype = 'data';
        for (let i = 0; i < 64; i++) {
            const sample = placement.sampleMatchingTypeComponent(
                component, idxEntry, 'coding');
            assert.ok(sample === 3);
        }

        /* Now sampling a coding endpoint is impossible */
        component.components[3].ftype = 'data';
        for (let i = 0; i < 64; i++) {
            const sample = placement.sampleMatchingTypeComponent(
                component, idxEntry, 'coding');
            assert.ok(sample === -1);
        }

        done();
    });

    mocha.describe('Flat topologies', function () {
        mocha.it('Not enough hyperdrives', function (done) {
            const policy = {
                cluster: {
                    components: [{
                        affinity: 'hard',
                        ftype: 'both',
                        name: 'hd1',
                    }],
                    dynamicSum: 100,
                    dynamicWeights: [100],
                    name: 'cluster',
                },
            };

            {
                const res = placement.select(policy, 2, 0);
                assert.strictEqual(res.dataLocations[0], 'hd1');
                assert.strictEqual(res.dataLocations[1], null);
            }
            {
                const res = placement.select(policy, 0, 2);
                assert.strictEqual(res.codingLocations[0], 'hd1');
                assert.strictEqual(res.codingLocations[1], null);
            }
            {
                const res = placement.select(policy, 1, 1);
                assert.strictEqual(res.dataLocations[0], 'hd1');
                assert.strictEqual(res.codingLocations[0], null);
            }

            done();
        });

        mocha.it('Soft functional type', function (done) {
            const policy = {
                cluster: {
                    components: [{
                        affinity: 'soft',
                        ftype: 'both',
                        name: 'hd1',
                    }],
                    dynamicSum: 100,
                    dynamicWeights: [100],
                    name: 'cluster',
                },
            };

            const res = placement.select(policy, 10, 10);
            assert.ok(res.dataLocations.every(e => e === 'hd1'));
            assert.ok(res.codingLocations.every(e => e === 'hd1'));
            done();
        });

        mocha.it('Different hyperdrives', function (done) {
            /* Here we enforce placing all coding fragments on the same hyperdrives */
            const policy = {
                cluster: {
                    components: [
                        { affinity: 'hard', ftype: 'data', name: 'hd1' },
                        { affinity: 'hard', ftype: 'coding', name: 'hd2' },
                        { affinity: 'hard', ftype: 'data', name: 'hd3' },
                        { affinity: 'hard', ftype: 'data', name: 'hd4' },
                        { affinity: 'hard', ftype: 'coding', name: 'hd5' },
                        { affinity: 'hard', ftype: 'data', name: 'hd6' },
                    ],
                    dynamicSum: 6 * 25,
                    dynamicWeights: [25, 25, 25, 25, 25, 25],
                    name: 'cluster',
                },
            };

            for (let i = 0; i < 64; i++) {
                const { dataLocations, codingLocations } = placement.select(policy, 2, 1);
                assert.ok(dataLocations[0] === 'hd1' ||
                          dataLocations[0] === 'hd3' ||
                          dataLocations[0] === 'hd4' ||
                          dataLocations[0] === 'hd6');
                assert.ok(dataLocations[1] === 'hd1' ||
                          dataLocations[1] === 'hd3' ||
                          dataLocations[1] === 'hd4' ||
                          dataLocations[1] === 'hd6');
                assert.ok(codingLocations[0] === 'hd2' ||
                          codingLocations[0] === 'hd5');
            }
            done();
        });
    });

    mocha.describe('Deep topologies', function () {
        mocha.it('Witness setup', function (done) {
            /* 3 sites:
             * - first 2 are regular ones, each holding a single
             *   data fragment
             * - witness site, holding the single coding
             */
            const policy = {
                cluster: {
                    components: [{
                        name: 'SiteA',
                        affinity: 'hard',
                        ftype: 'data',
                        dynamicSum: 2,
                        dynamicWeights: [1, 1],
                        components: [
                            { affinity: 'hard', ftype: 'both', name: 'SiteA-hd1' },
                            { affinity: 'hard', ftype: 'both', name: 'SiteA-hd2' },
                        ] }, {
                            name: 'SiteB',
                            affinity: 'hard',
                            ftype: 'data',
                            dynamicSum: 2,
                            dynamicWeights: [1, 1],
                            components: [
                                { affinity: 'hard', ftype: 'both', name: 'SiteB-hd1' },
                                { affinity: 'hard', ftype: 'both', name: 'SiteB-hd2' },
                            ] }, {
                                name: 'Witness',
                                affinity: 'hard',
                                ftype: 'coding',
                                dynamicSum: 2,
                                dynamicWeights: [1, 1],
                                components: [
                                    { affinity: 'hard', ftype: 'both', name: 'Witness-hd1' },
                                    { affinity: 'hard', ftype: 'both', name: 'Witness-hd2' },
                                ] }],
                    dynamicSum: 60,
                    dynamicWeights: [20, 20, 20],
                    name: 'cluster',
                },
            };

            for (let i = 0; i < 64; i++) {
                const { dataLocations, codingLocations } = placement.select(policy, 2, 1);
                assert.ok(dataLocations.some(e => e.startsWith('SiteA-hd')));
                assert.ok(dataLocations.some(e => e.startsWith('SiteB-hd')));
                assert.ok(codingLocations.some(e => e.startsWith('Witness-hd')));
            }
            done();
        });

        mocha.it('Free-form', function (done) {
            const policy = {
                cluster: {
                    components: [
                        {
                            name: 'SiteA',
                            affinity: 'soft',
                            ftype: 'data',
                            dynamicSum: 2,
                            dynamicWeights: [1, 1],
                            components: [
                                { affinity: 'hard', ftype: 'both', name: 'SiteA-hd1' },
                                { affinity: 'hard', ftype: 'both', name: 'SiteA-hd2' },
                            ],
                        },
                        { affinity: 'hard', ftype: 'coding', name: 'Witness-hd1' },
                    ],
                    dynamicSum: 3,
                    dynamicWeights: [2, 1],
                    name: 'cluster',
                },
            };

            for (let i = 0; i < 64; i++) {
                const { dataLocations, codingLocations } = placement.select(policy, 2, 1);
                assert.ok(dataLocations[0] === 'SiteA-hd1' ||
                          dataLocations[0] === 'SiteA-hd2');
                assert.ok(dataLocations[1] === 'SiteA-hd1' ||
                          dataLocations[1] === 'SiteA-hd2');
                assert.ok(dataLocations[0] !== dataLocations[1]);
                assert.strictEqual(codingLocations[0], 'Witness-hd1');
            }
            done();
        });
    });
});
