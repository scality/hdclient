'use strict'; // eslint-disable-line strict
/* eslint-disable max-len */
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');

const { placement, utils: libUtils } = require('../../index');

mocha.describe('Data placement', function () {
    const policy = {
        locations: libUtils.range(8).map(idx => `hyperdrive${idx}`),
    };

    mocha.it('Sanity check', function (done) {
        const { dataLocations, codingLocations } = placement.select(policy, 4, 2);
        const allLoc = [...dataLocations, ...codingLocations];
        assert.strictEqual(dataLocations.length, 4);
        assert.strictEqual(codingLocations.length, 2);

        // Assert all can be found
        allLoc.forEach(loc => {
            assert.ok(policy.locations.find(l => l === loc));
        });

        // Assert all of them are different
        const uniques = new Set(allLoc);
        assert.strictEqual(uniques.size, allLoc.length);
        done();
    });
});
