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
 * Validate configuration (types and values)
 *
 * @param {Object} opts Options to validate
 * @returns { [true, null] } if everything is ok
 * @returns { [boolean, Error] } false and a throwable
 *          customized error when invalid
 */
function validate(opts) {
    if (! (opts instanceof Object)) {
        return [false,
                new InvalidConfigError('', '', 'No options passed')];
    }

    if (! (opts.endpoints instanceof Array)) {
        return [false,
                new InvalidConfigError('endpoints',
                                       opts.endpoints,
                                       'Expected an array of string')];
    }

    if (opts.endpoints.some(elem => typeof(elem) !== 'string')) {
        return [false,
                new InvalidConfigError('endpoints',
                                       opts.endpoints,
                                       'Expected an array of string')];
    }

    if (opts.endpoints.length === 0) {
        return [false,
                new InvalidConfigError('endpoints',
                                       opts.endpoints,
                                       'Expected at least 1 endpoint')];
    }

    if (typeof(opts.dataParts) !== 'number' ||
        opts.dataParts < 1) {
        return [false,
                new InvalidConfigError('dataParts',
                                       opts.dataParts,
                                       'Expected integer larger than 1')];
    }

    if (typeof(opts.codingParts) !== 'number' ||
        opts.codingParts < 0) {
        return [false,
                new InvalidConfigError('codingParts',
                                       opts.codingParts,
                                       'Expected integer larger than 0')];
    }

    if (opts.endpoints.length < opts.dataParts + opts.codingParts) {
        return [false,
                new InvalidConfigError('totalParts',
                                       opts.dataParts + opts.codingParts,
                                       'Expected less parts than endpoints')];
    }

    if (typeof(opts.requestTimeoutMs) !== 'number' ||
        opts.requestTimeoutMs < 0) {
        return [false,
                new InvalidConfigError('requestTimeoutMs',
                                       opts.requestTimeoutMs,
                                       'Expected a positive number')];
    }

    return [true, null];
}

module.exports = {
    InvalidConfigError,
    validate,
};
