'use strict'; // eslint-disable-line strict

/**
 * regroup various helper code
 */

/**
 * Compare two HTTP errors
 *
 * @param {null|undefined|Error} lhs - Left hand side
 * @param {null|undefined|Error} rhs - Right hand side
 * @return {Number} ~ lhs - rhs
*/
function compareErrors(lhs, rhs) {
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

/**
 * Get an array filled with [0, n[
 *
 * @param {Number} n - Range upper end (exclusive)
 * @return {[Number]} range array
 */
function range(n) {
    /* Javascript... */
    return [...Array(n).keys()];
}

module.exports = {
    compareErrors,
    range,
};
