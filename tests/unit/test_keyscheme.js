'use strict'; // eslint-disable-line strict
/* eslint-disable max-len */
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');

const { keyscheme, utils: libUtils } = require('../../index');

mocha.describe('Keyscheme', function () {
    const policy = {
        locations: libUtils.range(10).map(
            idx => `hyperdrive${idx}):${idx}${idx}${idx}`),
    };

    mocha.describe('Keygen', function () {
        mocha.it('Basic', function (done) {
            const fragments = keyscheme.keygen(
                policy, 'testObj', 4096, 'RS', 2, 1, 314159
            );

            /* Verify globals */
            assert.strictEqual(fragments.objectKey, 'testObj');
            assert.strictEqual(fragments.code, 'RS');
            assert.strictEqual(fragments.nDataParts, 2);
            assert.strictEqual(fragments.nCodingParts, 1);
            assert.strictEqual(fragments.rand, 314159);
            assert.strictEqual(fragments.nChunks, 1);
            assert.strictEqual(fragments.splitSize, 0);

            /* Verify each fragment */
            assert.strictEqual(fragments.chunks.length, 1);
            assert.strictEqual(fragments.chunks[0].data.length, 2);
            assert.strictEqual(fragments.chunks[0].coding.length, 1);
            fragments.chunks[0].data.forEach((f, i) => {
                assert.strictEqual(f.type, 'd');
                assert.strictEqual(typeof f.port, 'number');
                assert.strictEqual(typeof f.hostname, 'string');
                assert.strictEqual(f.fragmentId, i);
                assert.strictEqual(
                    f.key, `testObj-314159-0-d-${i}`);
            });

            fragments.chunks[0].coding.forEach((f, i) => {
                assert.strictEqual(f.type, 'c');
                assert.strictEqual(typeof f.port, 'number');
                assert.strictEqual(typeof f.hostname, 'string');
                assert.strictEqual(f.fragmentId, 2 + i);
                assert.strictEqual(
                    f.key, `testObj-314159-0-c-${2 + i}`);
            });

            done();
        });

        mocha.it('Random part', function (done) {
            const fragments1 = keyscheme.keygen(
                policy, 'testObj', 123456, 'CP', 2, 0
            );
            const fragments2 = keyscheme.keygen(
                policy, 'testObj', 123456, 'CP', 2, 0
            );

            /* Rand part should be different */
            assert.ok(fragments1.rand !== fragments2.rand);

            /* Everything else must be equal */
            assert.strictEqual(fragments1.objectKey, fragments2.objectKey);
            assert.strictEqual(fragments1.code, fragments2.code);
            assert.strictEqual(fragments1.nDataParts, fragments2.nDataParts);
            assert.strictEqual(fragments1.nCodingParts, fragments2.nCodingParts);
            assert.strictEqual(fragments1.nChunks, fragments2.nChunks);
            assert.strictEqual(fragments1.splitSize, fragments2.splitSize);
            assert.strictEqual(fragments1.chunks.length,
                               fragments2.chunks.length);

            assert.strictEqual(fragments2.chunks[0].data.length,
                               fragments2.chunks[0].data.length);
            fragments1.chunks[0].data.forEach((f1, i) => {
                /* location, hostname and port might be different */
                const f2 = fragments2.chunks[0].data[i];
                assert.strictEqual(f1.type, f2.type);
                assert.strictEqual(f1.fragmentId, f2.fragmentId);
                /* key is different since it contains the random part */
                assert.ok(f1.key !== f2.key);
            });

            assert.strictEqual(fragments2.chunks[0].coding.length,
                               fragments2.chunks[0].coding.length);
            fragments1.chunks[0].coding.forEach((f1, i) => {
                /* location, hostname and port might be different */
                const f2 = fragments2.chunks[0].coding[i];
                assert.strictEqual(f1.type, f2.type);
                assert.strictEqual(f1.fragmentId, f2.fragmentId);
                /* key is different since it contains the random part */
                assert.ok(f1.key !== f2.key);
            });

            done();
        });
    });

    mocha.describe('(De)Serialize invariant', function () {
        ['CP', 'RS'].forEach(code => {
            for (let nData = 1; nData < 6; ++nData) {
                for (let nCoding = 0; nCoding < 3; ++nCoding) {
                    if (code === 'CP' && nCoding > 0) {
                        continue;
                    }

                    // TODO: test split cases

                    mocha.it(`Invariant ${code}${nData}${nCoding}`, function (done) {
                        const fragments = keyscheme.keygen(
                            policy, 'fake', 1024, code, nData, nCoding);
                        const serialized = keyscheme.serialize(fragments);
                        const parsed = keyscheme.deserialize(serialized);

                        const sections = serialized.split(keyscheme.SECTION_SEPARATOR);
                        assert.strictEqual(sections.length, 6 + nData + nCoding);
                        assert.strictEqual(sections[0], String(keyscheme.KEYSCHEME_VERSION));
                        assert.strictEqual(sections[1], String(keyscheme.TOPOLOGY_VERSION));

                        /* Check fragments === parsed */
                        assert.strictEqual(fragments.objectKey, parsed.objectKey);
                        assert.strictEqual(fragments.rand, parsed.rand);
                        assert.strictEqual(fragments.code, parsed.code);
                        assert.strictEqual(fragments.nDataParts, parsed.nDataParts);
                        assert.strictEqual(fragments.nCodingParts, parsed.nCodingParts);
                        assert.strictEqual(fragments.nChunks, parsed.nChunks);
                        assert.strictEqual(fragments.splitSize, parsed.splitSize);

                        assert.strictEqual(fragments.chunks.length, parsed.chunks.length);
                        fragments.chunks.forEach((chunk, chunkid) => {
                            assert.strictEqual(chunk.data.length,
                                               parsed.chunks[chunkid].data.length);
                            chunk.data.forEach((f, i) => {
                                const p = parsed.chunks[chunkid].data[i];
                                assert.strictEqual(f.type, p.type);
                                assert.strictEqual(f.fragmentId, p.fragmentId);
                                assert.strictEqual(f.key, p.key);
                                assert.strictEqual(f.hostname, p.hostname);
                                assert.strictEqual(f.port, p.port);
                                assert.strictEqual(f.location, p.location);
                            });

                            assert.strictEqual(chunk.coding.length,
                                               parsed.chunks[chunkid].coding.length);
                            chunk.coding.forEach((f, i) => {
                                const p = parsed.chunks[chunkid].coding[i];
                                assert.strictEqual(f.type, p.type);
                                assert.strictEqual(f.fragmentId, p.fragmentId);
                                assert.strictEqual(f.key, p.key);
                                assert.strictEqual(f.hostname, p.hostname);
                                assert.strictEqual(f.port, p.port);
                                assert.strictEqual(f.location, p.location);
                            });
                        });
                        done();
                    });
                }
            }
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

        mocha.it('Bad scheme topology version', function (done) {
            try {
                keyscheme.deserialize('1#gné#1,0#RS,2,1#genobj#123456#hd2#hd1#hd1');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Unknown topology version gné');
                done();
            }
        });
        mocha.it('Negative scheme topology version', function (done) {
            try {
                keyscheme.deserialize('1#-1#1,0#RS,2,1#genobj#123456#hd2#hd1#hd1');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Unknown topology version -1');
                done();
            }
        });
        mocha.it('Future scheme topolgy version', function (done) {
            try {
                keyscheme.deserialize('1#2#1,0#RS,2,1#genobj#123456#hd2#hd1#hd1');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Unknown topology version 2');
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

        mocha.it('No object key section', function (done) {
            try {
                keyscheme.deserialize('1#1#split#code#');
                done(new Error('Shoud never have been reached'));
            } catch (err) {
                assert.ok(err instanceof keyscheme.KeySchemeDeserializeError);
                assert.strictEqual(err.message, 'Bad key: no object key section');
                done();
            }
        });

        mocha.it('No rand section', function (done) {
            try {
                keyscheme.deserialize('1#1#split#code#obj#');
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
