'use strict'; // eslint-disable-line strict
/* eslint-disable no-console */

/**
 * Utility to parse keys generated on PUT
 */

const { keyscheme } = require('../index');

function main() {
    if (process.argv.length < 3) {
        const scriptName = process.argv[1].split('/').slice(-1)[0];
        console.error(`Usage: ${scriptName} <key>`);
        process.exit(1);
    }

    console.log(JSON.stringify(
        keyscheme.deserialize(process.argv[2])));
}

/* If run as a script */
if (typeof require !== 'undefined' && require.main === module) {
    main();
}
