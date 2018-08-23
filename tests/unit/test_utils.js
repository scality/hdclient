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
});
