'use strict'; // eslint-disable-line strict

/**
 * Module in charge of picking new chunk/part keys and
 * their endpoints, aggregating those 'sub keys' into a
 * single 'rawKey', serializing and deserializing raw keys
 *
 * Version 1 scheme grammar:
 * <rawKey> := <scheme_version>#[<uri>#]<uri>
 * <uri> := <part_key>/<hostname>/<port>
 * <hostname> := String ENdpoint DSN resolvable id
 * <port> := Integer Which port to contact
 * <part_key> := <seed>-<timestamp>-<part_id>-<type>
 * <seed> := Integer (in decimal)
 * <timestamp> := Integer (in decimal) - Unix epoch
 * <part_id> := Integer (in decimal) - 0 for first fragment, 1 for next, etc.
 * <type> := data | coding
 */

/**
 * Keyscheme is versioned, to easily support changes.
 * Each generated rawKey embeds the keygen version.
 */
const KEYSCHEME_VERSION = '1';

/**
 * Separator used to mark sections in rawKey
 */
const SECTION_SEPARATOR = '#';

/**
 * Separator used to mark sub sections in rawKey
 */
const SUB_SECTION_SEPARATOR = '/';

/**
 * Separator used to structure the part/fragment key.
 * This key is the one actually sent and stored in
 * the hyperdrives
 */
const PART_KEY_SEPARATOR = '-';

/* TODO if we don't store directly endpoints of each chunk,
 * then endpoints topology MUST be versioned as well.
 */

class KeySchemeDeserializeError extends Error {
    constructor(message) {
        super(message);
        // Saving class name in the property of our custom error as a shortcut.
        this.name = this.constructor.name;
        // Capturing stack trace, excluding constructor call from it.
        Error.captureStackTrace(this, this.constructor);
    }
}

/**
 * Generate a random part key
 *
 * @param {Number} partId to use in the key
 * @param {Number} seed Can be used for all keys to share a prefix
 * @returns {String} Generated key
 */
function partKeygen(partId, seed) {
    const timestamp = Date.now();
    return [seed, timestamp, partId].join(PART_KEY_SEPARATOR);
}

/**
 * Select endpoints to contact to put new keys, and generate them
 *
 * @param {String[]} endpoints Cluster topology to use
 * @param {String} objectKey Real object key
 * @param {Number} nDataParts Number of data endpoints to select
 * @param {Number} nCodingParts Number of coding endpoints to select
 * @param {Number|String} seed Can be used for all keys to share a prefix
 * @returns {Object} parts Part locations
 * @returns {Object[]} parts.data Location of each data fragments
 * @returns {Object[]} parts.coding Location of each coding fragments
 *
 * Each fragment is an object wit the following attributes:
 * - hostname {String}
 * - port {Number}
 * - type {String} ('data' or 'coding') Redundant, but debug ease
 * - partId {Number} index of current part in [0, nDataParts + nCodingParts[
 * - key {String} part identifier (stored on the drive)
 *
 * @comment Validated configuration enforces we have enough endpoints
 * @comment If seed is not set, a random value in [0, 2**32[ is chosen
 */
function keygen(endpoints, objectKey, nDataParts, nCodingParts,
                seed = Math.floor(Math.random() * 4294967296)) {
    /* Simple implementation: pick 1 according to uniform sampling
     * over |endpoints|, and the nDataParts + nCodingParts following.
     * Guarantees each part ends up on a diffrent hyperdrive
     */
    const parts = {
        objectKey,
        nDataParts,
        nCodingParts,
        data: [],
        coding: [],
    };

    const len = endpoints.length;
    let pos = Math.floor(Math.random() * len);
    for (let i = 0; i < nDataParts; ++i) {
        const [hostname, port] = endpoints[pos].split(':');
        parts.data.push({
            hostname,
            port: Number(port),
            type: 'data',
            partId: i,
            key: partKeygen(i, seed),
        });
        pos = (pos + 1) % len;
    }
    for (let i = nDataParts; i < nDataParts + nCodingParts; ++i) {
        const [hostname, port] = endpoints[pos].split(':');
        parts.coding.push({
            hostname,
            port: Number(port),
            type: 'coding',
            partId: i,
            key: partKeygen(i, seed),
        });
        pos = (pos + 1) % len;
    }
    return parts;
}

/**
 * Serialize a fragment/part/chunk into a uri
 *
 * @param {Object} A part object
 * @param {String} type Set to 'data' or 'coding'
 * @returns {String} Serialized part uri
 */
function serializePart({ key, hostname, port }, type) {
    return [key, hostname, port, type].join(SUB_SECTION_SEPARATOR);
}

/**
 * Serialize all parts into a object uri, enabling retrieval of each parts
 *
 * @param {Object} parts description. Refer to selectEndpoints
 *                 for inside details
 * @returns {String} Object uri
 */
function serialize(parts) {
    const dataKeys = parts.data.map(elem => serializePart(elem, 'data'))
          .join(SECTION_SEPARATOR);
    const codingKeys = parts.coding.map(elem => serializePart(elem, 'coding'))
          .join(SECTION_SEPARATOR);
    return `${KEYSCHEME_VERSION}${SECTION_SEPARATOR}${dataKeys}${codingKeys}`;
}

/**
 * Extract frament/part information from serialized version
 *
 * @param {String} rawPart to parse
 * @returns {Object} Part location information
 * @returns {String} Part type ('data' or 'coding')
 */
function deserializePart(rawPart) {
    const [key, hostname, port, type] = rawPart.split(SUB_SECTION_SEPARATOR);
    return [{ key, hostname, port: Number(port) }, type];
}

/**
 * Deserialize a raw key (ie object uri) into its part description
 *
 * @param {String} rawKey to parse
 * @returns {Object} Part objects.
 * @throws DeserializationError if anything goes south
 * @comment Returned object has same structure as the one returned
 *          by selectEndpoints, with additional key on each part.
 */
function deserialize(rawKey) {
    const [version, ...rawParts] = rawKey.split(SECTION_SEPARATOR);
    if (version !== KEYSCHEME_VERSION) {
        throw new KeySchemeDeserializeError(`Unknown version ${version}`);
    }

    if (rawParts.length === 0) {
        throw new KeySchemeDeserializeError('No parts found');
    }

    const deserialized = {
        data: [],
        coding: [],
    };

    rawParts.forEach(elem => {
        const [part, type] = deserializePart(elem);
        deserialized[type].push(part);
    });

    deserialized.nDataParts = deserialized.data.length;
    deserialized.nCodingParts = deserialized.coding.length;
    return deserialized;
}

module.exports = {
    KeySchemeDeserializeError,
    keygen,
    serialize,
    deserialize,
};
