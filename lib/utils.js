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

/**
 * Extract the range to GET inide a chunk, gival object-wide range
 *
 * @param {Objects} fragments - Parsed/generated key
 * @param {Number} chunkId - Which chunk are we analyzing
 * @param {null|[Number]} globalRange - object-level GET request parameter
 * @return {Object} key with {use: Boolean, range: [Number, Number]| undefined}
 * @comment If use if false, we don't have to retrieve this chunk
 */
function getChunkRange(fragments, chunkId, globalRange) {
    if (globalRange === null ||
        globalRange === undefined ||
        fragments.nChunks === 1) {
        return { use: true, chunkRange: globalRange }; // All
    }

    const splitSize = fragments.splitSize;
    const start = chunkId * splitSize;
    const end = Math.min(fragments.size,
                         (chunkId + 1) * splitSize - 1);

    const gstart = Math.max(globalRange[0], 0);
    const gend = globalRange.length === 2 ?
              globalRange[1] : fragments.size;

    if (end <= gstart) {
        /* Start is after the range */
        return { use: false };
    } else if (gend < start) {
        /* Requested ends before this chunk */
        return { use: false };
    }

    const cstart = Math.max(start, gstart);
    const cend = Math.min(end, gend);
    return { use: true, chunkRange: [cstart - start, cend - start] };
}

/**
 * Resolve UUID into host:port
 *
 * @param {Object} uuidmapping - Map UUIDS to hyperdrive endpoints (ip:port)
 * @param {String} uuid - UUID to resolve
 * @return {null|Object} null on error, {hostname, port} on success
 */
function resolveUUID(uuidmapping, uuid) {
    const match = uuidmapping[uuid];
    if (match) {
        const [hostname, port] = match.split(':');
        return { hostname, port: Number(port) };
    }

    return null;
}


module.exports = {
    compareErrors,
    getChunkRange,
    range,
    resolveUUID,
};
