'use strict'; // eslint-disable-line strict
/* eslint-disable max-len */
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');

const { keyscheme, utils: libUtils, split } = require('../../index');


/* Make sure we are using the fake placement selector */
// eslint-disable-next-line no-unused-vars
const testUtils = require('../utils');

function getPlacementPolicy(minSplitSize = 0) {
    return {
        minSplitSize,
        locations: libUtils.range(10).map(idx => `fakeUUID-${idx}`),
    };
}

mocha.describe('Keyscheme', function () {
    mocha.describe('Keygen', function () {
        mocha.it('Basic', function (done) {
            const keyContext = { bucketName: 'test', objectKey: 'veryFake Object', version: 2 };
            const policy = getPlacementPolicy();
            const fragments = keyscheme.keygen(
                1, policy, keyContext, split.DATA_ALIGN, 'RS', 2, 1);

            /* Verify globals */
            assert.strictEqual(fragments.code, 'RS');
            assert.strictEqual(fragments.nDataParts, 2);
            assert.strictEqual(fragments.nCodingParts, 1);
            assert.strictEqual(fragments.stripeSize, split.DATA_ALIGN);
            assert.strictEqual(fragments.nChunks, 1);
            assert.strictEqual(fragments.size, split.DATA_ALIGN);
            assert.strictEqual(fragments.splitSize, split.DATA_ALIGN);

            /* Verify each fragment */
            assert.strictEqual(fragments.chunks.length, 1);
            assert.strictEqual(fragments.chunks[0].data.length, 2);
            assert.strictEqual(fragments.chunks[0].coding.length, 1);
            fragments.chunks[0].data.forEach((f, i) => {
                assert.strictEqual(typeof f.uuid, 'string');
                assert.strictEqual(f.fragmentId, i);
                assert.strictEqual(
                    f.key, `1-${fragments.ctime}-${fragments.rand}-${fragments.size}-${i}`);
            });

            fragments.chunks[0].coding.forEach((f, i) => {
                assert.strictEqual(typeof f.uuid, 'string');
                assert.strictEqual(f.fragmentId, 2 + i);
                assert.strictEqual(
                    f.key, `1-${fragments.ctime}-${fragments.rand}-${fragments.size}-${2 + i}`);
            });

            done();
        });

        mocha.it('Randomness section', function (done) {
            const keyContext = { bucketName: 'test', objectKey: 'veryFake Object', version: 2 };
            const policy = getPlacementPolicy();
            const fragments1 = keyscheme.keygen(
                1, policy, keyContext, split.DATA_ALIGN, 'RS', 2, 1);
            const fragments2 = keyscheme.keygen(
                1, policy, keyContext, split.DATA_ALIGN, 'RS', 2, 1);

            /* Verify randomness */
            assert.ok(fragments1.rand !== fragments2.rand);
            done();
        });

        mocha.it('Split', function (done) {
            // final splitSize should be aligned
            const serviceId = 42;
            const keyContext = { bucketName: 'test', objectKey: 'veryFake Object', version: 2 };
            const policy = getPlacementPolicy(split.DATA_ALIGN * 4 - 1);
            const fragments = keyscheme.keygen(
                serviceId,
                policy,
                keyContext,
                split.DATA_ALIGN * 8 + 1, // Tough luck, worst possible overhead
                'CP', 3, 0, 314159
            );

            /* Verify globals */
            assert.strictEqual(fragments.serviceId, 42);
            assert.strictEqual(fragments.code, 'CP');
            assert.strictEqual(fragments.nDataParts, 3);
            assert.strictEqual(fragments.nCodingParts, 0);
            assert.strictEqual(fragments.stripeSize, 0);
            assert.strictEqual(fragments.nChunks, 3);
            assert.strictEqual(fragments.size, split.DATA_ALIGN * 8 + 1);
            assert.strictEqual(fragments.splitSize, split.DATA_ALIGN * 4);

            /* Verify each fragment */
            assert.strictEqual(fragments.chunks.length, 3);
            fragments.chunks.forEach((chunk, chunkId) => {
                const endOffset = Math.min(fragments.size, fragments.splitSize * (chunkId + 1));
                assert.strictEqual(chunk.data.length, 3);
                assert.strictEqual(chunk.coding.length, 0);
                chunk.data.forEach((f, i) => {
                    assert.strictEqual(typeof f.uuid, 'string');
                    assert.strictEqual(f.fragmentId, i);
                    assert.strictEqual(
                        f.key, `${serviceId}-${fragments.ctime}-${fragments.rand}-${endOffset}-${i}`);
                });
            });

            done();
        });
    });

    mocha.describe('(De)Serialize invariant', function () {
        [16384, 5000, 333].forEach(splitSize => {
            ['CP', 'RS'].forEach(code => {
                for (let nData = 1; nData < 6; ++nData) {
                    for (let nCoding = 0; nCoding < 3; ++nCoding) {
                        if (code === 'CP' && nCoding > 0) {
                            continue;
                        }
                        mocha.it(`Invariant ${code}${nData}${nCoding}`, function (done) {
                            const keyContext = { bucketName: 'test', objectKey: 'veryFake Object', version: 2 };
                            const policy = getPlacementPolicy(splitSize);
                            const fragments = keyscheme.keygen(
                                42, policy, keyContext, 10000, code, nData, nCoding);
                            const serialized = keyscheme.serialize(fragments);
                            const parsed = keyscheme.deserialize(serialized);

                            const sections = serialized.split(keyscheme.SECTION_SEPARATOR);
                            assert.strictEqual(sections.length, 6 + nData + nCoding);
                            assert.strictEqual(sections[0], String(keyscheme.KEYSCHEME_VERSION));
                            assert.strictEqual(sections[1], String(42));

                            /* Check fragments === parsed */
                            assert.deepStrictEqual(parsed, fragments);
                            done();
                        });
                    }
                }
            });
        });
    });

    mocha.describe('Deserialize error', function () {
        mocha.it('Bad scheme version', function (done) {
            try {
                keyscheme.deserialize('gné#1#1,0#RS,2,1#genobj#123456#hd2#hd1#hd1');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Unknown version gné');
                done();
            }
        });

        mocha.it('Negative scheme version', function (done) {
            try {
                keyscheme.deserialize('-1#1#1,0#RS,2,1#genobj#123456#hd2#hd1#hd1');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Unknown version -1');
                done();
            }
        });

        mocha.it('Future scheme version', function (done) {
            try {
                keyscheme.deserialize('2#1#1,0#RS,2,1#genobj#123456#hd2#hd1#hd1');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Unknown version 2');
                done();
            }
        });

        mocha.it('Bad serviceId', function (done) {
            try {
                keyscheme.deserialize('1#gné#1,0#RS,2,1#genobj#123456#hd2#hd1#hd1');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Unknown serviceId gné');
                done();
            }
        });

        mocha.it('Negative serviceId', function (done) {
            try {
                keyscheme.deserialize('1#-1#1,0#RS,2,1#genobj#123456#hd2#hd1#hd1');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Unknown serviceId -1');
                done();
            }
        });

        mocha.it('No split section', function (done) {
            try {
                keyscheme.deserialize('1#1#');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Bad key: no split section');
                done();
            }
        });

        mocha.it('No replication policy section', function (done) {
            try {
                keyscheme.deserialize('1#1#split#');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Bad key: no replication policy section');
                done();
            }
        });

        mocha.it('No ctime section', function (done) {
            try {
                keyscheme.deserialize('1#1#split#code#');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Bad key: no ctime section');
                done();
            }
        });

        mocha.it('No rand section', function (done) {
            try {
                keyscheme.deserialize('1#1#split#code#ctime#');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Bad key: no rand section');
                done();
            }
        });

        mocha.it('Not enough locations: replication', function (done) {
            try {
                keyscheme.deserialize('1#1#split#CP,2#obj#123#hd1');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Found 1 parts, expected CP,2');
                done();
            }
        });

        mocha.it('Not enough locations: erasure coding', function (done) {
            try {
                keyscheme.deserialize('1#1#split#RS,4,2#obj#123#hd1#hd2#hd3#hd4#hd5');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Found 5 parts, expected RS,4,2');
                done();
            }
        });

        mocha.it('Bad split section', function (done) {
            try {
                keyscheme.deserialize('1#1#whatever-gné#RS,4,1#obj#123#hd1#hd2#hd3#hd4#hd5');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Failed to deserialize split section: whatever-gné');
                done();
            }
        });
    });
});
