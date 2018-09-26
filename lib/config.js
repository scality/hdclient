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
 * Validate policy configuration (types and values)
 *
 * @param {Object}policy Policy section to validate
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

    if (! (policy.locations instanceof Array)) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'policy.locations',
                policy.locations,
                'Expected an array of string'),
        };
    }

    if (policy.locations.some(elem => typeof(elem) !== 'string')) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'policy.locations',
                policy.locations,
                'Expected an array of string'),
        };
    }

    if (policy.locations.length === 0) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'policy.locations',
                policy.locations,
                'Expected at least 1 endpoint'),
        };
    }

    if (typeof(policy.minSplitSize) !== 'number' ||
        policy.minSplitSize < 0) {
        // eslint-disable-next-line no-param-reassign
        policy.minSplitSize = 0; // no split
    }

    return { configIsValid: true, configError: null };
}

/**
 * Validate configuration (types and values)
 *
 * @param {Object} opts Options to validate
 * @returns { Object } with decision and error if invalid
 * @returns { boolean } Object.configIsValid
 * @returns { null | InvalidConfigError } Object.ConfigError
 */
function validate(opts) {
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

    if (typeof(opts.dataParts) !== 'number' ||
        opts.dataParts < 1) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'dataParts',
                opts.dataParts,
                'Expected integer larger than 1'),
        };
    }

    if (typeof(opts.codingParts) !== 'number' ||
        opts.codingParts < 0) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'codingParts',
                opts.codingParts,
                'Expected integer larger than 0'),
        };
    }

    if (opts.policy.locations.length < opts.dataParts + opts.codingParts) {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'totalParts',
                opts.dataParts + opts.codingParts,
                'Expected less parts than data locations'),
        };
    }

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

    if (!opts.errorAgent || typeof(opts.errorAgent.kafkaBrokers) !== 'string') {
        return {
            configIsValid: false,
            configError: new InvalidConfigError(
                'errorAgent.kafkaBrokers',
                opts.errorAgent,
                'Expected a CSV list of hostnames'),
        };
    }

    return { configIsValid: true, configError: null };
}

module.exports = {
    InvalidConfigError,
    validate,
};
