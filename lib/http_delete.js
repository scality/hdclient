'use strict'; // eslint-disable-line strict

const assert = require('assert');

const protocol = require('./protocol');
const httpUtils = require('./http_utils');

/**
 * Delete all fragments of an object
 *
 * @param {http.HttpAgent} httpAgent - Agent to use
 * @param {werelogs.Logger} logger - Logger to use
 * @param {Object} fragments - Object description to delete
 * @param {HyperdriveClient~deleteCallback} callback - Callback
 * @param {Number} requestTimeoutMs - Timeout of each sub-query
 * @returns {Object} Operation context tracking everything
 */
function doDELETE({ httpAgent, log, fragments, callback, requestTimeoutMs }) {
    // Split, replication or erasure coding is currently not supported
    assert.strictEqual(fragments.nChunks, 1);
    assert.strictEqual(fragments.nCodingParts, 0);

    const opContext = httpUtils.makeOperationContext(fragments);

    /* Send all at once */
    fragments.chunks.forEach((chunk, chunkId) => {
        [...chunk.data, ...chunk.coding].forEach((fragment, fragmentId) => {
            const reqContext = { opContext, chunkId, fragmentId };
            const { hostname, port, key } = fragment;
            const requestOptions = httpUtils.getCommonStoreRequestOptions(
                httpAgent, hostname, port, key);

            requestOptions.method = 'DELETE';
            requestOptions.headers = {
                ['Content-Length']: 0,
                ['Accept']: protocol.helpers.makeAccept(),
            };

            const request = httpUtils.newRequest(
                requestOptions, log, reqContext, requestTimeoutMs,
                /* callback */
                opCtx => {
                    log.end();
                    const status =
                              opCtx.status[chunkId].statuses[fragmentId];
                    return callback(status.error);
                });

            request.end();
        });
    });

    return opContext;
}

module.exports = doDELETE;
