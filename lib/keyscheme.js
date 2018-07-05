'use strict'; // eslint-disable-line strict

/**
 * Module in charge of picking new chunk/part keys and
 * their endpoints, aggregating those 'sub keys' into a
 * single 'rawKey', serializing and deserializing raw keys
 *
 * Version 1 scheme grammar:
 * <genkey> := <version>#<topologyVersion>#<split>#<repPolicy># \
*                  <objectKey>#<rand>#<location>[#<location>]
 * <version> := Natural (so 0 or 1 to start)
 * <topologyVersion> := Natural (so 0 or 1 to start) - if we use indirection
 *                      instead of storing
 * <split> := <nChunk>%<splitSize>
 * <nChunk> := number of splitted parts (1 for non-splitted objects)
 * <splitSize> := size of each splitted parts, except last one
*                 (see hyperdrive keys below)
 * <repPolicy> := RSk+m or CPn (for n-1 copies)
 * <objectKey> := parent S3 object key (or a prefix) - can contains anything but
 *                section and sub-section separators
 * <rand> := 64 bits random number (unicity inside 1 hyperdrive)
 * <location>:= hyperdrive location (UUID, idx in table?, ip:port, ...)
 *
 * Actual, stored hyperdrive keys can be easily computed from <genkey>.
 * <storedFragmentKey> := <objectKey>-<rand>-<startOffset>-<type>-<fragid>
 * <objectKey> and <rand> are the ones defined above
 * <type> := ‘d’ for data, ‘c’ for coding
 * <fragid>:= index in main key fragment list
 * <startOffset> := used for splits. All split chunks share the same prefix,
 *                  storing the offset is used to easily have range queries
 *                  and avoid storing them all in the main key.
 */

const assert = require('assert');
const placement = require('./placement');

/**
 * Separator used to mark sections in rawKey
 */
const SECTION_SEPARATOR = '#';

/**
 * Separator used to mark sub sections in rawKey
 */
const SUBSECTION_SEPARATOR = ',';

/**
 * Separator used to structure the part/fragment key.
 * This key is the one actually sent and stored in
 * the hyperdrives
 */
const PART_KEY_SEPARATOR = '-';

/**
 * Keyscheme is versioned, to easily support changes.
 * Each generated rawKey embeds the keygen version.
 */
const KEYSCHEME_VERSION = 1;

/* TODO if we don't store directly endpoints of each chunk,
 * then endpoints topology MUST be versioned as well.
 */
const TOPOLOGY_VERSION = 1;

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
 * @param {String} prefix of fragment key
 * @param {Number} rand value to avoid same object name conficts
 * @param {Number} startOffset inside the whole object (see split Design.md)
 * @param {String} type data 'd' or coding 'c'
 * @param {Number} fragmentId to use in the key
 * @returns {String} Generated key
 */
function fragmentKeygen(prefix, rand, startOffset, type, fragmentId) {
    return [prefix, rand, startOffset,
            type, fragmentId].join(PART_KEY_SEPARATOR);
}

/**
 * Helper function to create fragments
 *
 * @param {Object} context of all fragments
 * @param {String} location ip:port of destination
 * @param {String} type data 'd' or coding 'c'
 * @param {Number} fragmentId position in the used code
 * @returns {Object} Generated fragment object
 */
function makeFragment(context, location, type, fragmentId) {
    const [hostname, port] = location.split(':');
    return {
        location,
        type,
        fragmentId,
        hostname,
        port: Number(port),
        key: fragmentKeygen(context.objectKey, context.rand,
                            0, type, fragmentId),
    };
}

/**
 * Select endpoints to contact to put new keys, and generate them
 *
 * @param {Object} policy Data placement policy
 * @param {String} objectKey Real object key
 * @param {String} code to use (RS or CP for replication - copy)
 * @param {Number} nDataParts Number of data endpoints to select
 * @param {Number} nCodingParts Number of coding endpoints to select
 * @param {Number} rand Used for same object name deduplication
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
function keygen(policy, objectKey, code, nDataParts, nCodingParts,
                rand = Math.floor(Math.random() * 4294967296)) {
    const parts = {
        objectKey,
        rand,
        code,
        nDataParts,
        nCodingParts,
        splitSize: 0,
    };

    const [dataLoc, codingLoc] =
              placement.select(policy, nDataParts, nCodingParts);
    parts.data = dataLoc.map(
        (e, i) => makeFragment(parts, e, 'd', i));
    parts.coding = codingLoc.map(
        (e, i) => makeFragment(parts, e, 'c', nDataParts + i));
    return parts;
}

/**
 * Serialize split section of main generated key
 *
 * @param {Object} parts description
 * @returns {String} serialized section
 */
function serializeSplitSection(parts) {
    /* Split not supported yet */
    const nChunk = 1;
    return `${nChunk}${SUBSECTION_SEPARATOR}${parts.splitSize}`;
}

function serializeReplicationPolicySection(parts) {
    // Erasure coding not supported yet
    const subsection = [parts.code, parts.nDataParts];
    if (parts.code === 'RS') {
        subsection.push(parts.nCodingParts);
    }
    return subsection.join(SUBSECTION_SEPARATOR);
}

/**
 * Serialize all parts into a object uri, enabling retrieval of each parts
 *
 * @param {Object} parts description. Refer to keygen
 *                 for inside details
 * @returns {String} Object uri
 */
function serialize(parts) {
    const dataLocations = parts.data.map(elem => elem.location);
    const codingLocations = parts.coding.map(elem => elem.location);
    return [
        KEYSCHEME_VERSION,
        TOPOLOGY_VERSION,
        serializeSplitSection(parts),
        serializeReplicationPolicySection(parts),
        parts.objectKey,
        parts.rand,
        ...dataLocations,
        ...codingLocations,
    ].join(SECTION_SEPARATOR);
}

/**
 * Deserialize a raw key (ie object uri) into its part description
 *
 * @param {String} rawKey to parse
 * @returns {Object} Part objects.
 * @throws DeserializationError if anything goes south
 * @comment Returned object has same structure as the one returned
 *          by keygen.
 */
function deserialize(rawKey) {
    const [versionStr, topoVersionStr, split, repPolicy,
           objectKey, rand, ...locations] =
              rawKey.split(SECTION_SEPARATOR);

    const version = parseInt(versionStr, 10);
    if (isNaN(version) ||
        version < 1 ||
        version > KEYSCHEME_VERSION) {
        throw new KeySchemeDeserializeError(`Unknown version ${versionStr}`);
    }

    const topoVersion = parseInt(topoVersionStr, 10);
    if (isNaN(topoVersion) ||
        topoVersion < 1 ||
        topoVersion > TOPOLOGY_VERSION) {
        throw new KeySchemeDeserializeError(
            `Unknown topology version ${topoVersionStr}`);
    }
    if (!split) {
        throw new KeySchemeDeserializeError('Bad key: no split section');
    }
    if (!repPolicy) {
        throw new KeySchemeDeserializeError(
            'Bad key: no replication policy section'
        );
    }
    if (!objectKey) {
        throw new KeySchemeDeserializeError('Bad key: no object key section');
    }

    if (!rand) {
        throw new KeySchemeDeserializeError('Bad key: no rand section');
    }

    const [code, nDataPartsStr, nCodingPartsStr] =
              repPolicy.split(SUBSECTION_SEPARATOR);
    const nDataParts = parseInt(nDataPartsStr, 10);
    const nCodingParts = nCodingPartsStr ? parseInt(nCodingPartsStr, 10) : 0;

    if (locations.length !== nDataParts + nCodingParts) {
        const msg = `Found ${locations.length} parts, expected ${repPolicy}`;
        throw new KeySchemeDeserializeError(msg);
    }

    const parsed = {
        objectKey,
        code,
        nDataParts,
        nCodingParts,
        rand: parseInt(rand, 10),
        splitSize: 0, // TODO
    };

    parsed.data = locations.slice(0, nDataParts).map(
        (loc, idx) => makeFragment(parsed, loc, 'd', idx));
    assert.strictEqual(parsed.data.length, nDataParts);

    parsed.coding = locations.slice(nDataParts).map(
        (loc, idx) => makeFragment(parsed, loc, 'c', nDataParts + idx));
    assert.strictEqual(parsed.coding.length, nCodingParts);

    return parsed;
}

module.exports = {
    KeySchemeDeserializeError,
    keygen,
    serialize,
    deserialize,
    KEYSCHEME_VERSION,
    TOPOLOGY_VERSION,
    SECTION_SEPARATOR,
    SUBSECTION_SEPARATOR,
    PART_KEY_SEPARATOR,
};
