'use strict'; // eslint-disable-line strict

/**
 * regroup hleper code related to error handling
 */

/**
 * Compare two HTTP errors
 *
 * @param {null|undefined|Error} lhs - Left hand side
 * @param {null|undefined|Error} rhs - Right hand side
 * @return {Number} ~ lhs - rhs
*/
function compare(lhs, rhs) {
    const noLhs = (lhs === undefined || lhs === null);
    const noRhs = (rhs === undefined || rhs === null);

    if (noLhs && noRhs) {
        return 0;
    } else if (noRhs) {
        return 1;
    } else if (noLhs) {
        return -1;
    }

    return lhs.infos.status - rhs.infos.status;
}

module.exports = {
    compare,
};
