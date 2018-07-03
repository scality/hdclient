'use strict'; // eslint-disable-line strict

/** Useful protocol specification, tools and others
 *
 * Protocol specification can be found here:
 *
 * Hyperdrive-specific modifications can be found in Scality RING
 * repository under modules/hyperdrive/hyperiod/README.md
 *
 * Note: until hyperdrive is merged into a realse branch, you must
 * checkout the branch feature/RING-21232-hyperdrive
 */

const assert = require('assert');

// Queries are created with http.request which hardcodedly uses only HTTP 1.1

/**
 * All key operations must be performed under this url prefix
 */
const STORAGE_BASE_URL = '/store';

const GET_QUERY_MANDATORY_HEADERS = ['Accept'];
const GET_REPLY_MANDATORY_HEADERS = ['Content-Length', 'Content-Type'];
const PUT_QUERY_MANDATORY_HEADERS = ['Content-Length', 'Content-Type'];
/* Hyperdrive returns specific Content-Type info on PUT
 * for RING backward compatibilty reasons.
 * Note: not used here
 */
const PUT_REPLY_MANDATORY_HEADERS = ['Content-Length', 'Content-Type'];
const DELETE_QUERY_MANDATORY_HEADERS = ['Accept', 'Content-Length'];
/* Hyperdrive can return specific info on DELETE
 * for RING backward compatibilty reasons.
 * Note: not used here
 */
const DELETE_REPLY_MANDATORY_HEADERS = ['Content-Length'];

/** As per the spec, Content-Type header must always starts with
 * the following pattern.
 */
const HYPERDRIVE_APPLICATION = 'application/x-scality-storage-data';

/**
 * Hyperdrive supports (?) internal sampled tracing
 * Should the feature be activated, sending any query
 * with header like X-Scal-Trace-Ids: <trace_id>-<span_id>,
 * where trace and span id are positive numbers strictly lower
 * than 2**64, will follow the processing through every layer
 * of the hyperdrive.
 *
 * Output can be used to perform distribued tracing, very much
 * like Google's Dapper or OpenTracing
 */
const REQUEST_TRACING_HEADER = 'X-Scal-Trace-Ids';

/**
 * Hyperdrive support multi-payload queries
 * Currently supports 4 types.
 * 'crc' is treated specially, and is used on GET to request
 * hyperdrive to write returned stream CRCs, which can then be
 * compared with announced CRCs to detect corruption.
 */
const SUPPORTED_PAYLOAD_TYPES = ['data', 'usermeta', 'meta', 'crc'];

/**
 * Transform the different payload information for PUT query
 * into a hyperdrive valid Content-Type string
 * @param {Number} payload.data Positive data payload length
 * @param {Number} payload.usermeta Positive user metadata payload length
 * @param {Number} payload.meta Positive metadata payload length
 * @returns {String} Valid Content-Type string to use for PUT query
 */
function makePutContentType({ data, usermeta, meta }) {
    const dataType = (data !== null && data !== undefined) ?
          `; data=${data}` : '';
    const usermetaType = (usermeta !== null && usermeta !== undefined) ?
          `; usermeta=${usermeta}` : '';
    const metaType = (meta !== null && meta !== undefined) ?
          `; meta=${meta}` : '';
    return HYPERDRIVE_APPLICATION.concat(dataType, usermetaType, metaType);
}

/**
 * Parse Content-Type header returned by PUT query
 * @param {String} contentType header
 * @returns {Map} keyeed with the different types
 */
function parseReturnedContentType(contentType) {
    const [app, ...payloads] = contentType.split(';');
    assert.strictEqual(app, HYPERDRIVE_APPLICATION);
    return new Map(payloads.map(entry => {
        const [ptype, value] = entry.split('=');
        return [ptype.trim(), Number(value)];
    }));
}

/**
 * Transform the different payload information for PUT query
 * into a hyperdrive valid Content-Type string
 * @param {[[String, Range]]} payloads - typically [["data", [12]], ["meta"]]
 * @returns {String} Valid Content-Type string to use for GET query
 * @returns {Null} if passed payloads are invalid types
 */
function makeAccept(...payloads) {
    const mismatchingTypes = function mismatch(payload) {
        return undefined === SUPPORTED_PAYLOAD_TYPES.find(
            entry => entry === payload[0]);
    };
    if (payloads.some(mismatchingTypes)) {
        return null;
    }

    const encodedPayloads = payloads.map(entry => {
        const [ptype, range] = entry;
        if (!range) {
            return `${ptype}`;
        } else if (range.length === 1) {
            return `${ptype}=${range[0]}-`;
        }
        return `${ptype}=${range[0]}-${range[1]}`;
    });

    return [HYPERDRIVE_APPLICATION, ...encodedPayloads].join('; ');
}

module.exports = {
    specs: {
        STORAGE_BASE_URL,
        GET_QUERY_MANDATORY_HEADERS,
        GET_REPLY_MANDATORY_HEADERS,
        PUT_QUERY_MANDATORY_HEADERS,
        PUT_REPLY_MANDATORY_HEADERS,
        DELETE_QUERY_MANDATORY_HEADERS,
        DELETE_REPLY_MANDATORY_HEADERS,
        HYPERDRIVE_APPLICATION,
        REQUEST_TRACING_HEADER,
        SUPPORTED_PAYLOAD_TYPES,
    },
    helpers: {
        makePutContentType,
        parseReturnedContentType,
        makeAccept,
    },
};
