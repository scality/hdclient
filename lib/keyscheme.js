'use strict'; // eslint-disable-line strict

/**
 * Module in charge of picking new chunk/part keys and
 * their endpoints, aggregating those 'sub keys' into a
 * single 'rawKey', serializing and deserializing raw keys
 *
 * Version 1 scheme grammar:
 * <genkey> := <version>#<placementPolicyVersion>#<split>#<repPolicy># \
*                  <objectKey>#<rand>#<location>[#<location>]
 * <version> := Natural (so 0 or 1 to start)
 * <placementPolicyVersion> := Natural (so 0 or 1 to start) -
                               what placement policy was used
 * <split> := <size>,<splitSize>
 * <size> := total size of the object
 * <splitSize> := size of each splitted parts, except last one
 *                 (see hyperdrive keys below)
 * <repPolicy> := RS,k,m,stripeSize or CPY,n (for n-1 copies)
 * <objectKey> := parent S3 object key (or a prefix) - can contains anything but
 *                section and sub-section separators
 * <rand> := 32 bits pseudo random number (unicity inside 1 hyperdrive),
             encoded as hexadecimal
 * <location>:= hyperdrive location (UUID, idx in table?, ip:port, ...)
 *
 * Actual, stored hyperdrive keys can be easily computed from <genkey>.
 * <storedFragmentKey> := <objectKey>-<rand>-<startOffset>- \
               <placementPolicyVersion>-<repPolicy>-<fragid>
 * <placementPolicyVersion>, <repPolicy>, <objectKey> and <rand>
 * are the ones defined above.
 * <fragid>:= index in main key fragment list
 * <startOffset> := used for splits. All split chunks share the same prefix,
 *                  storing the offset is used to easily have range queries
 *                  and avoid storing them all in the main key.
 */

const assert = require('assert');
const placement = require('./placement');
const split = require('./split');
const utils = require('./utils');

// Overriden in tests to make them deterministic
let locationSelector = placement.select;

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
 * Serialize split section of main generated key
 *
 * @param {Object} parts description
 * @returns {String} serialized section
 */
function serializeSplitSection(parts) {
    return `${parts.size}${SUBSECTION_SEPARATOR}${parts.splitSize}`;
}

/**
 * Serialize replication policy section
 *
 * @param {Object} parts description
 * @returns {String} serialized section
 */
function serializeReplicationPolicySection(parts) {
    // Erasure coding not supported yet
    const subsection = [parts.code, parts.nDataParts];
    if (parts.code === 'RS') {
        subsection.push(parts.nCodingParts);
        subsection.push(parts.stripeSize);
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
    return [
        KEYSCHEME_VERSION,
        placement.PLACEMENT_POLICY_VERSION,
        serializeSplitSection(parts),
        serializeReplicationPolicySection(parts),
        parts.objectKey,
        parts.rand,
        ...parts.dataLocations,
        ...parts.codingLocations,
    ].join(SECTION_SEPARATOR);
}

/**
 * Generate a random part key
 *
 * @param {String} prefix of fragment key
 * @param {Number} rand value to avoid same object name conficts
 * @param {Number} startOffset inside the whole object (see split Design.md)
 * @param {String} layout Stringified replication policy
 * @param {Number} fragmentId to use in the key
 * @returns {String} Generated key
 */
function fragmentKeygen(prefix, rand, startOffset, layout, fragmentId) {
    return [prefix.slice(0, 20), rand, startOffset,
            placement.PLACEMENT_POLICY_VERSION,
            layout, fragmentId].join(PART_KEY_SEPARATOR);
}

/**
 * Helper function to create fragments
 *
 * @param {Object} context of all fragments
 * @param {String} uuid of destination
 * @param {Number} startOffset inside the whole object (see split Design.md)
 * @param {String} layout Stringified replication policy
 * @param {Number} fragmentId position in the used code
 * @returns {Object} Generated fragment object
 */
function makeFragment(context, uuid, startOffset, layout, fragmentId) {
    return {
        fragmentId,
        uuid,
        key: fragmentKeygen(context.objectKey, context.rand,
                            startOffset, layout, fragmentId),
    };
}

/**
 * Select endpoints to contact to put new keys, and generate them
 *
 * @param {Object} policy Data placement policy
 * @param {String} objectKey Real object key
 * @param {Number} size Real object size
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
function keygen(policy, objectKey, size, code, nDataParts, nCodingParts,
                rand = Math.floor(Math.random() * 4294967296)) {
    const { nChunks, splitSize, stripeSize } = split.getSplitSize(
        policy.minSplitSize, size, code, nDataParts);
    const { dataLocations, codingLocations } =
              locationSelector(policy, nDataParts, nCodingParts);

    const parts = {
        objectKey,
        rand: rand.toString(16),
        code,
        nDataParts,
        nCodingParts,
        nChunks,
        size,
        splitSize,
        stripeSize,
        dataLocations,
        codingLocations,
    };

    const repPolicy = serializeReplicationPolicySection(parts);
    parts.chunks = utils.range(nChunks).map(chunkid => {
        const startOffset = splitSize * chunkid;
        const data = dataLocations.map(
            (e, i) => makeFragment(parts, e, startOffset,
                                   repPolicy, i));
        const coding = codingLocations.map(
            (e, i) => makeFragment(parts, e, startOffset,
                                   repPolicy, nDataParts + i));
        return { data, coding };
    });

    return parts;
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
    const [versionStr, topoVersionStr, splitSection, repPolicy,
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
        topoVersion > placement.PLACEMENT_POLICY_VERSION) {
        throw new KeySchemeDeserializeError(
            `Unknown topology version ${topoVersionStr}`);
    }
    if (!splitSection) {
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

    const [code, nDataPartsStr, nCodingPartsStr, stripeSizeStr] =
              repPolicy.split(SUBSECTION_SEPARATOR);
    const nDataParts = parseInt(nDataPartsStr, 10);
    const nCodingParts = nCodingPartsStr ? parseInt(nCodingPartsStr, 10) : 0;
    const stripeSize = stripeSizeStr ? parseInt(stripeSizeStr, 10) : 0;

    if (locations.length !== nDataParts + nCodingParts) {
        const msg = `Found ${locations.length} parts, expected ${repPolicy}`;
        throw new KeySchemeDeserializeError(msg);
    }

    const dataLocations = locations.slice(0, nDataParts);
    assert.strictEqual(dataLocations.length, nDataParts);
    const codingLocations = locations.slice(nDataParts);
    assert.strictEqual(codingLocations.length, nCodingParts);

    const [sizeStr, splitSizeStr] =
              splitSection.split(SUBSECTION_SEPARATOR);
    const size = parseInt(sizeStr, 10);
    const splitSize = parseInt(splitSizeStr, 10);
    if (isNaN(size) || isNaN(splitSize) ||
        size < splitSize || splitSize <= 0) {
        const msg = `Failed to deserialize split section: ${splitSection}`;
        throw new KeySchemeDeserializeError(msg);
    }
    const nChunks = Math.ceil(size / splitSize);

    const parsed = {
        objectKey,
        code,
        nDataParts,
        nCodingParts,
        size,
        splitSize,
        stripeSize,
        nChunks,
        dataLocations,
        codingLocations,
        rand,
    };

    parsed.chunks = utils.range(nChunks).map(chunkid => {
        const startOffset = splitSize * chunkid;
        const data = dataLocations.map(
            (e, i) => makeFragment(
                parsed, e, startOffset, repPolicy, i));
        const coding = codingLocations.map(
            (e, i) => makeFragment(
                parsed, e, startOffset, repPolicy, nDataParts + i));
        return { data, coding };
    });

    return parsed;
}

function updateLocationSelector(newCallback) {
    locationSelector = newCallback;
}

module.exports = {
    updateLocationSelector, // Override to use a custom endpoint selection
    KeySchemeDeserializeError,
    keygen,
    serialize,
    deserialize,
    KEYSCHEME_VERSION,
    SECTION_SEPARATOR,
    SUBSECTION_SEPARATOR,
    PART_KEY_SEPARATOR,
};
