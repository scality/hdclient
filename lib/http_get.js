'use strict'; // eslint-disable-line strict

const assert = require('assert');

const protocol = require('./protocol');
const httpUtils = require('./http_utils');

/**
 * Get all fragments of an object
 *
 * @param {http.HttpAgent} httpAgent - Agent to use
 * @param {Object} errorAgent - Agent to persist errors
 * @param {werelogs.Logger} logger - Logger to use
 * @param {Object} fragments - Object description to delete
 * @param {String} rawKey - Uri of the object
 *                 (refer to keyscheme.js for content)
 * @param {HyperdriveClient~getCallback} callback - Callback
 * @param {Number} requestTimeoutMs - Timeout of each sub-query
 * @param {Number [] | Undefined} range - Range (if any) with
 *                                        first element the start
 *                                        and the second element the end
 * @returns {Object} Operation context tracking everything
 */
function doGET({ httpAgent, errorAgent, log, fragments, rawKey,
                 callback, requestTimeoutMs, range }) {
    // Split, replication or erasure coding is currently not supported
    assert.strictEqual(fragments.nChunks, 1);
    assert.strictEqual(fragments.nCodingParts, 0);

    const opContext = httpUtils.makeOperationContext(fragments, rawKey, log);

    /* Request first chunk all at once
     * TODO: split? Request all? First 2 then the next when
     *       first is consumed? Introduced a small delay (1ms)?
     * TODO: handle range on split
     */
    fragments.chunks.slice(0, 1).forEach((chunk, chunkId) => {
        [...chunk.data, ...chunk.coding].forEach((fragment, fragmentId) => {
            const reqContext = { opContext, chunkId, fragmentId };
            const { hostname, port, key } = fragment;
            const requestOptions = httpUtils.getCommonStoreRequestOptions(
                httpAgent, hostname, port, key);

            requestOptions.method = 'GET';
            requestOptions.headers = {
                ['Accept']: protocol.helpers.makeAccept(['data', range]),
            };

            const request = httpUtils.newRequest(
                requestOptions, log, reqContext,
                requestTimeoutMs,
                /* callback */
                reqCtx => {
                    const opCtx = reqCtx.opContext;
                    opCtx.log.end();
                    const status =
                              opCtx.status[chunkId].statuses[fragmentId];
                    return callback(status.error, status.response);
                });

            request.end();
        });
    });

    return opContext;
}

module.exports = {
    doGET,
};
