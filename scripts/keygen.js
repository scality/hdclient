'use strict'; // eslint-disable-line strict
/* eslint-disable no-console */
/* eslint-disable max-len */

/**
 * Utility to parse keys generated on PUT
 */

const assert = require('assert');
const fs = require('fs');
const { keyscheme, config } = require('../index');

function main() {
    if (process.argv.length < 6) {
        const scriptName = process.argv[1].split('/').slice(-1)[0];
        console.error(
            `${scriptName} <hdc conf> <RS,k,m or CP,n> <bucket/object/version> <size> [<rand>]`
        );
        process.exit(1);
    }

    const hdconf = JSON.parse(fs.readFileSync(process.argv[2]));
    config.validate(hdconf);

    const [code, nDataStr, nCodingStr] = process.argv[3].split(
        keyscheme.SUBSECTION_SEPARATOR);
    const nData = Number(nDataStr);
    const nCoding = nCodingStr ? Number(nCodingStr) : 0;
    assert.ok(nData > 0);
    assert.ok(nCoding >= 0);

    const keyContextArray = process.argv[4].split('/');
    const keyContext = {
        bucketName: keyContextArray[0],
        objectKey: keyContextArray[1],
        version: keyContextArray.length === 3 ? keyContextArray[2] : '1',
    };

    const parts = keyscheme.keygen(
        hdconf.serviceId,
        hdconf.policy,
        keyContext,
        process.argv[5],
        code, nData, nCoding,
        process.argv[6]);

    const genkey = keyscheme.serialize(parts);
    const output = {
        parts,
        genkey,
    };

    console.log(JSON.stringify(output));
}

/* If run as a script */
if (typeof require !== 'undefined' && require.main === module) {
    main();
}
