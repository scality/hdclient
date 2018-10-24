'use strict'; // eslint-disable-line strict
/* eslint-disable max-len */
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');

const { utils: libUtils } = require('../../index');

mocha.describe('Helper functions', function () {
    mocha.describe('Chunk range', function () {
        mocha.it('No range', function (done) {
            let res = libUtils.getChunkRange(null, 0, undefined);
            assert.ok(res.use);
            assert.strictEqual(res.chunkRange, undefined);

            res = libUtils.getChunkRange(null, 0, null);
            assert.ok(res.use);
            assert.strictEqual(res.chunkRange, null);
            done();
        });

        mocha.it('Single chunk', function (done) {
            const fragments = { nChunks: 1 };
            const range = [123, 4444];
            const res = libUtils.getChunkRange(fragments, 0, range);
            assert.ok(res.use);
            assert.deepStrictEqual(res.chunkRange, range);
            done();
        });

        mocha.it('No overlap - requested range after chunk', function (done) {
            const fragments = {
                nChunks: 2,
                size: 1024,
                splitSize: 512,
            };
            const range = [666, 1023];
            const { use } = libUtils.getChunkRange(fragments, 0, range);
            assert.strictEqual(use, false);
            done();
        });

        mocha.it('No overlap - requested range before chunk', function (done) {
            const fragments = {
                nChunks: 2,
                size: 1024,
                splitSize: 512,
            };
            const range = [1, 500];
            const { use } = libUtils.getChunkRange(fragments, 1, range);
            assert.strictEqual(use, false);
            done();
        });

        mocha.it('Multiple ranges', function (done) {
            const fragments = {
                nChunks: 3,
                size: 3000,
                splitSize: 1000,
            };
            const range = [900, 2001];
            {
                const { use, chunkRange } = libUtils.getChunkRange(fragments, 0, range);
                assert.strictEqual(use, true);
                assert.deepStrictEqual(chunkRange, [900, 999]);
            }
            {
                const { use, chunkRange } = libUtils.getChunkRange(fragments, 1, range);
                assert.strictEqual(use, true);
                assert.deepStrictEqual(chunkRange, [0, 999]);
            }
            {
                const { use, chunkRange } = libUtils.getChunkRange(fragments, 2, range);
                assert.strictEqual(use, true);
                assert.deepStrictEqual(chunkRange, [0, 1]);
            }
            done();
        });
    });

    mocha.it('Shallow array copy', function (done) {
        const original = [1, '2', true, { test: 42 }, [1, 2, 3]];
        const copy = libUtils.copyArray(original);
        copy[0] = 3.14159;
        copy[1] = 'paf!';
        copy[2] = {};
        copy[3] = null;
        copy[4] = [777];

        assert.strictEqual(original[0], 1);
        assert.strictEqual(original[1], '2');
        assert.strictEqual(original[2], true);
        assert.deepStrictEqual(original[3], { test: 42 });
        assert.deepStrictEqual(original[4], [1, 2, 3]);
        done();
    });

    mocha.describe('Categorical distribution sampling', function () {
        const nTrialsSmall = 100;
        const nTrialsBig = 10000;

        mocha.it('Collapsed distribution', function (done) {
            for (let i = 0; i < nTrialsSmall; i++) {
                const sample = libUtils.categoricalSample([1.0]);
                assert.strictEqual(0, sample);
            }
            for (let i = 0; i < nTrialsSmall; i++) {
                const sample = libUtils.categoricalSample([0, 0, 1, 0.0]);
                assert.strictEqual(2, sample);
            }
            done();
        });

        mocha.it('Weird behavior - passing wrong normalization hint', function (done) {
            const badConstant = 1e-8;
            const weights = [0.5, 0.5];
            const counts = new Array(2).fill(0);
            for (let i = 0; i < nTrialsBig; i++) {
                const sample = libUtils.categoricalSample(weights, badConstant);
                counts[sample] += 1;
            }
            /* In this case, all samples are skewed onto entry 0 */
            assert.ok(counts[0] > nTrialsBig * 0.999);
            done();
        });

        mocha.it('Impossible - passing no weights', function (done) {
            assert.strictEqual(libUtils.categoricalSample([]), null);
            done();
        });

        mocha.it('Random categorical distribution', function (done) {
            const nCategories = 10;
            const scale = Math.random() * 10000;
            const weights = libUtils.range(nCategories).map(() => Math.random() * scale);
            const sum = weights.reduce((a, b) => a + b, 0.0);
            const counts = new Array(nCategories).fill(0);
            for (let i = 0; i < nTrialsBig; i++) {
                const sample = libUtils.categoricalSample(weights, sum);
                counts[sample] += 1;
            }

            /* Check distribution against expected sample ratios */
            counts.forEach((c, i) => {
                const expectedRatio = weights[i] / sum;
                const ratio = c / nTrialsBig;
                assert.ok(Math.abs(expectedRatio - ratio) < 0.01);
            });
            done();
        });
    });
});
