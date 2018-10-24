'use strict'; // eslint-disable-line strict

/**
 * Configuration error
 */
class InvalidConfigError extends Error {
    constructor(option, value, detail) {
        // Calling parent constructor of base Error class.
        super(`Bad value: ${option}=${value} - ${detail}`);

        // Saving class name in the property of our custom error as a shortcut.
        this.name = this.constructor.name;

        // Capturing stack trace, excluding constructor call from it.
        Error.captureStackTrace(this, this.constructor);
    }
}

/**
 * Retrieve affinity field (hard|soft), handling defaults
 *
 * @param { Object } component Entry to inspect
 * @returns { null | String } affinity or null on error
 */
function getAffinity(component) {
    if (component.affinity === undefined ||
        component.affinity === null) {
        const isDeepestLevel = !component.components;
        /* Be conservative but still forgiving
         * Lowest level (hyperdrive) must default to hard, since
         * loosing an hyperdrive means loosing more fragments than
         * expected. Use case mostly used for dev/test I guess...
         *
         * Higher in the hierarchy should default to soft since
         * it is most likely the settings we want (multiple fragments
         * on a site for example).
         */
        return isDeepestLevel ? 'hard' : 'soft';
    } else if (component.affinity !== 'soft' &&
               component.affinity !== 'hard') {
        return null;
    }

    return component.affinity;
}

/**
 * Retrieve ftype field (data|coding|both), handling defaults
 *
 * @param { Object } component Entry to inspect
 * @returns { null | String } fragment type or null on error
 */
function getFragmentType(component) {
    if (component.ftype === undefined ||
        component.ftype === null) {
        return 'both'; // accept data and coding fragments
    } else if (component.ftype !== 'data' &&
               component.ftype !== 'coding' &&
               component.ftype !== 'both') {
        return null;
    }

    return component.ftype;
}

/**
 * Validate static weight of an hyperdrive
 *
 * @param { Object } component Entry to inspect
 * @returns { null | InvalidConfigError } null if OK, config error otherwise
 */
function validateWeight(component) {
    if (typeof(component.staticWeight) !== 'number' ||
        component.staticWeight < 0) {
        return new InvalidConfigError(
            'component.staticWeight',
            component.staticWeight,
            'Static weight must be a positive number');
    }

    return null;
}

/**
 * Validate leaf of the cluster description (hyperdrive)
 *
 * @param { Object } hyperdrive Leaf to validate
 * @returns { Object } with parsed section, decision and error if invalid
 * @returns { Object|null|undefined } Object.config parsed/enhanced object
 * @returns { boolean } Object.configIsValid
 * @returns { null | InvalidConfigError } Object.ConfigError
 */
function validateHyperdriveDescription(hyperdrive) {
    if (typeof(hyperdrive.name) !== 'string') {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'hyperdrive.name',
                hyperdrive.name,
                'Deepest level require a unique name field (UUID)'
            ),
        };
    }

    const enhanced = {
        name: hyperdrive.name,
        decomissionned: !!hyperdrive.decomissionned,
    };
    enhanced.ftype = getFragmentType(hyperdrive);
    if (!enhanced.ftype) {
        const configError = new InvalidConfigError(
            'hyperdrive.ftype', hyperdrive.ftype,
            'ftype field expects either "data", "coding" or "both"');
        return { configIsValid: false, configError };
    }

    enhanced.affinity = getAffinity(hyperdrive);
    if (!enhanced.affinity) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'hyperdrive.affinity', hyperdrive.affinity,
                'affinity expects either "soft" or "hard" as value'
            ) };
    }

    const badWeight = validateWeight(hyperdrive);
    if (badWeight) {
        return { configIsValid: false, configError: badWeight };
    }
    enhanced.staticWeight = hyperdrive.staticWeight;
    enhanced.dynamicWeights = [hyperdrive.staticWeight];
    enhanced.dynamicSum = hyperdrive.staticWeight;

    return { config: enhanced,
             configIsValid: true,
             configError: null };
}

/**
 * Recursively validate and enrich cluster description
 *
 * @param { Object} component Entry to inspect
 * @param { Number } depth Tree traversal current depth
 * @param { Number } idx Tree traversal children index (idx in parent's array)
 * @returns { Object } with parsed section, decision and error if invalid
 * @returns { Object|null|undefined } Object.config parsed/enhanced object
 * @returns { boolean } Object.configIsValid
 * @returns { null | InvalidConfigError } Object.ConfigError
*/
function recurseValidateCluster(component, depth, idx) {
    // Matching deepest components: hyperdrives
    if (!component.components) {
        return validateHyperdriveDescription(component);
    }

    const name = component.name || `Internal-${depth}-${idx}`;
    const enhanced = { name,
                       affinity: getAffinity(component),
                       ftype: getFragmentType(component),
                     };

    if (!enhanced.affinity) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'component.affinity', component.affinity,
                'affinity expects either "soft" or "hard" as value'
            ) };
    }
    if (!enhanced.ftype) {
        const configError = new InvalidConfigError(
            'component.ftype', component.ftype,
            'ftype field expects either "data", "coding" or "both"');
        return { configIsValid: false, configError };
    }

    // Recursive check
    enhanced.components = [];
    for (let i = 0; i < component.components.length; i++) {
        const validated = recurseValidateCluster(
            component.components[i], depth + 1, i);
        if (!validated.configIsValid) {
            return validated;
        }
        enhanced.components.push(validated.config);
    }

    // Weight of component assigned from weight of children
    enhanced.dynamicWeights = enhanced.components.map(c => c.dynamicSum);
    enhanced.dynamicSum = enhanced.dynamicWeights.reduce((a, b) => a + b, 0);
    if (enhanced.dynamicSum <= 0) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'Aggregated weights',
                enhanced.dynamicSum,
                // eslint-disable-next-line max-len
                'A container must have at least 1 sub-components with a non-zero staticWeight'
            ),
        };
    }

    return { config: enhanced, configIsValid: true, configError: null };
}

/**
 * Validate policy configuration (types and values)
 *
 * @param { Object } policy Policy section to validate
 * @returns { Object } with parsed section, decision and error if invalid
 * @returns { Object|null|undefined } Object.config parsed/enhanced object
 * @returns { boolean } Object.configIsValid
 * @returns { null | InvalidConfigError } Object.ConfigError
 */
function validatePolicySection(policy) {
    if (! (policy instanceof Object)) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'policy',
                policy,
                'Expected data placement policy'),
        };
    }

    if (! (policy.cluster instanceof Object)) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'policy.cluster',
                policy.cluster,
                'Expected a cluster topology object description'),
        };
    }

    if (! (policy.cluster.components instanceof Array)) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'policy.cluster.components',
                policy.cluster.components,
                'A clsuter expects at least 1 described component'),
        };
    }

    const clusterValidation = recurseValidateCluster(policy.cluster, 0, 0);
    if (!clusterValidation.configIsValid) {
        return clusterValidation;
    }
    const config = { cluster: clusterValidation.config };

    if (typeof(policy.minSplitSize) !== 'number' ||
        policy.minSplitSize < 0) {
        config.minSplitSize = 0; // no split
    } else {
        config.minSplitSize = policy.minSplitSize;
    }

    return { config, configIsValid: true, configError: null };
}

/**
 * Validate code configuration (types and values)
 *
 * @param { Object } codes Code section to validate
 * @returns { Object } with parsed section, decision and error if invalid
 * @returns { Object|null|undefined } Object.config parsed/enhanced object
 * @returns { boolean } Object.configIsValid
 * @returns { null | InvalidConfigError } Object.ConfigError
 */
function validateCodeSection(codes) {
    if (! (codes instanceof Array)) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'codes',
                codes,
                'Expected an array of { pattern, dataParts, codingParts }'),
        };
    }

    if (codes.length === 0) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'codes',
                codes,
                'Expected at least one code pattern'),
        };
    }

    const copied = [];
    for (let i = 0; i < codes.length; i++) {
        const code = codes[i];
        const codeCopy = Object.assign({}, code);

        if (typeof(code.type) !== 'string' ||
            (code.type !== 'CP' && code.type !== 'RS')) {
            return {
                configIsValid: false,
                configError: new InvalidConfigError(
                    'type',
                    code.type,
                    `Unknown code type (code ${i})`),
            };
        }

        if (typeof(code.dataParts) !== 'number' ||
            code.dataParts < 1) {
            return {
                configIsValid: false,
                configError: new InvalidConfigError(
                    'dataParts',
                    code.dataParts,
                    `Expected integer strictly larger than 0 (code ${i})`),
            };
        }

        if (typeof(code.codingParts) !== 'number' ||
            code.codingParts < 0) {
            return {
                configIsValid: false,
                configError: new InvalidConfigError(
                    'codingParts',
                    code.codingParts,
                    `Expected integer larger than 0 (code ${i})`),
            };
        }

        if (code.type === 'CP' && code.codingParts > 0) {
            return {
                configIsValid: false,
                configError: new InvalidConfigError(
                    'codingParts',
                    code.codingParts,
                    `Code type CP expects 0 coding parts (code ${i})`),
            };
        }

        if (typeof(code.pattern) !== 'string') {
            return {
                configIsValid: false,
                configError: new InvalidConfigError(
                    'pattern',
                    code.pattern,
                    `Expected a bucket/object regex pattern (code ${i})`),
            };
        }

        codeCopy.regex = new RegExp(`^${code.pattern}$`, 'u');
        copied.push(codeCopy);
    }

    return { config: copied, configIsValid: true, configError: null };
}

/**
 * Validate configuration (types and values)
 *
 * @param { Object } opts Options to validate
 * @returns { Object } with parsed section, decision and error if invalid
 * @returns { Object|null|undefined } Object.config parsed/enhanced object
 * @returns { boolean } Object.configIsValid
 * @returns { null | InvalidConfigError } Object.ConfigError
 */
function validate(opts) {
    const config = {};

    if (! (opts instanceof Object)) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                '', '', 'No options passed'),
        };
    }

    const policyValidated = validatePolicySection(opts.policy);
    if (!policyValidated.configIsValid) {
        return policyValidated;
    }
    config.policy = policyValidated.config;

    const codeValidated = validateCodeSection(opts.codes);
    if (!codeValidated.configIsValid) {
        return codeValidated;
    }
    config.codes = codeValidated.config;

    if (typeof(opts.requestTimeoutMs) !== 'number' ||
        opts.requestTimeoutMs < 0) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'requestTimeoutMs',
                opts.requestTimeoutMs,
                'Expected a positive number'),
        };
    }
    config.requestTimeoutMs = opts.requestTimeoutMs;

    if (!opts.errorAgent || typeof(opts.errorAgent.kafkaBrokers) !== 'string') {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'errorAgent.kafkaBrokers',
                opts.errorAgent,
                'Expected a CSV list of hostnames'),
        };
    }

    config.serviceId = opts.serviceId || 1;
    config.immutable = opts.immutable || true;
    return { config, configIsValid: true, configError: null };
}

module.exports = {
    InvalidConfigError,
    validatePolicySection,
    validateCodeSection,
    validate,
};
