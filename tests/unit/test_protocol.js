'use strict'; // eslint-disable-line strict
/* eslint-disable max-len */
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');
const { protocol: hdProto } = require('../../index');


mocha.describe('Hyperdrive Protocol Specification', function () {
    mocha.describe('GET Content-Type', function () {
        mocha.it('Empty', function (done) {
            const expected = `${hdProto.specs.HYPERDRIVE_APPLICATION}`;
            const generated = hdProto.helpers.makeAccept();
            assert.strictEqual(expected, generated);
            done();
        });
        mocha.it('Data only', function (done) {
            const expected = `${hdProto.specs.HYPERDRIVE_APPLICATION};data`;
            const generated = hdProto.helpers.makeAccept('data');
            assert.strictEqual(expected, generated);
            done();
        });
        mocha.it('Usermd only', function (done) {
            const expected = `${hdProto.specs.HYPERDRIVE_APPLICATION};usermeta`;
            const generated = hdProto.helpers.makeAccept('usermeta');
            assert.strictEqual(expected, generated);
            done();
        });
        mocha.it('Metadata only', function (done) {
            const expected = `${hdProto.specs.HYPERDRIVE_APPLICATION};meta`;
            const generated = hdProto.helpers.makeAccept('meta');
            assert.strictEqual(expected, generated);
            done();
        });
        mocha.it('Mixture', function (done) {
            // Usermd does not exists, it is usermeta
            const expected = `${hdProto.specs.HYPERDRIVE_APPLICATION};usermeta;data;meta`;
            const generated = hdProto.helpers.makeAccept('usermeta', 'data', 'meta');
            assert.strictEqual(expected, generated);
            done();
        });
        mocha.it('Failure', function (done) {
            const expected = null;
            const generated = hdProto.helpers.makeAccept('fake');
            assert.strictEqual(expected, generated);
            done();
        });
        mocha.it('Failure mixture', function (done) {
            // Usermd does not exists, it is usermeta
            const expected = null;
            const generated = hdProto.helpers.makeAccept('usermd', 'data');
            assert.strictEqual(expected, generated);
            done();
        });
    });

    mocha.describe('PUT Content-Type', function () {
        mocha.it('Empty', function (done) {
            const expected = `${hdProto.specs.HYPERDRIVE_APPLICATION}`;
            const generated = hdProto.helpers.makePutContentType({});
            assert.strictEqual(expected, generated);
            done();
        });
        mocha.it('Data only', function (done) {
            const expected = `${hdProto.specs.HYPERDRIVE_APPLICATION};data=65536`;
            const generated = hdProto.helpers.makePutContentType({ data: 65536 });
            assert.strictEqual(expected, generated);
            done();
        });
        mocha.it('Usermd only', function (done) {
            const expected = `${hdProto.specs.HYPERDRIVE_APPLICATION};usermeta=1024`;
            const generated = hdProto.helpers.makePutContentType({ usermeta: 1024 });
            assert.strictEqual(expected, generated);
            done();
        });
        mocha.it('Metadata only', function (done) {
            const expected = `${hdProto.specs.HYPERDRIVE_APPLICATION};meta=32`;
            const generated = hdProto.helpers.makePutContentType({ meta: 32 });
            assert.strictEqual(expected, generated);
            done();
        });
        mocha.it('Mixture', function (done) {
            // Usermd does not exists, it is usermeta
            const expected = `${hdProto.specs.HYPERDRIVE_APPLICATION};data=4096;usermeta=256;meta=11`;
            const generated = hdProto.helpers.makePutContentType(
                { usermeta: 256, data: 4096, meta: 11 }
            );
            assert.strictEqual(expected, generated);
            done();
        });
        mocha.it('Failure', function (done) {
            const expected = `${hdProto.specs.HYPERDRIVE_APPLICATION}`;
            const generated = hdProto.helpers.makePutContentType({ fake: 3141591 });
            assert.strictEqual(expected, generated);
            done();
        });
        mocha.it('Failure mixture', function (done) {
            // Usermd does not exists, it is usermeta
            const expected = `${hdProto.specs.HYPERDRIVE_APPLICATION};data=7777`;
            const generated = hdProto.helpers.makePutContentType({ usermd: 58, data: 7777 });
            assert.strictEqual(expected, generated);
            done();
        });
    });
});
