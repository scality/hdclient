'use strict'; // eslint-disable-line strict

const assert = require('assert');

const protocol = require('./protocol');
const httpUtils = require('./http_utils');

/**
 * Put all fragments of an object
 *
 * @param {http.HttpAgent} httpAgent - Agent to use
 * @param {werelogs.Logger} logger - Logger to use
 * @param {Object} fragments - Object description to delete
 * @param {String} rawGenKey - Uri of the object
     *                 (refer to keyscheme.js for content)
 * @param {HyperdriveClient~putCallback} callback - Callback
 * @param {Number} requestTimeoutMs - Timeout of each sub-query
 * @param {Number} size - Stream length
 * @param {stream.Readable} stream - Stream to store
 * @returns {Object} Operation context tracking everything
 */
function doPUT({ httpAgent, log, fragments, rawGenKey,
                 callback, requestTimeoutMs,
                 size, stream }) {
    // Split, replication or erasure coding is currently not supported
    assert.strictEqual(fragments.nChunks, 1);
    assert.strictEqual(fragments.nCodingParts, 0);

    const opContext = httpUtils.makeOperationContext(fragments);

    /* TODO: split & erasure coding */

    /* Dispatch replication */
    fragments.chunks.slice(0, 1).forEach((chunk, chunkId) => {
        chunk.data.forEach((fragment, fragmentId) => {
            const reqContext = { opContext, chunkId, fragmentId };
            const { hostname, port, key } = fragment;
            const contentType = protocol.helpers.makePutContentType(
                { data: size } /* Only 'data' payload is supported */
            );
            const requestOptions = httpUtils.getCommonStoreRequestOptions(
                httpAgent, hostname, port, key);

            requestOptions.method = 'PUT';
            requestOptions.headers = {
                ['Content-Length']: size,
                ['Content-Type']: contentType,
            };

            const request = httpUtils.newRequest(
                requestOptions, log, reqContext, requestTimeoutMs,
                /* callback */
                opCtx => {
                    log.end();
                    const endStatus =
                              opCtx.status[chunkId].statuses[fragmentId];
                    if (endStatus.timeout) {
                        return callback(null, rawGenKey);
                    }

                    return callback(endStatus.error, rawGenKey);
                });

            // TODO abstract a bit stream to have a safer interface
            stream.pipe(request);
            stream.on('error', err => {
                // forward error downstream
                request.emit('error', err);
            });
        });
    });

    return opContext;
}

module.exports = doPUT;
