'use strict'; // eslint-disable-line strict
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');
const HyperdriveClient = require('../../index');


mocha.describe('Hyperdrive Client', function () {
    mocha.it('create', function (done) {
        const hdclient = new HyperdriveClient();
        assert.ok(hdclient);
        done();
    });
});
