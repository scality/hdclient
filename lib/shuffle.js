'use strict'; // eslint-disable-line strict
Object.defineProperty(exports, "__esModule", { value: true });
const randomBytes = require('crypto').randomBytes;
/*
 * This set of function allows us to create an efficient shuffle
 * of our array, since Math.random() will not be enough (32 bits of
 * entropy are less than enough when the entropy needed is the factorial
 * of the array length).
 *
 * Many thanks to @jmunoznaranjo for providing us with a solid solution.
 */
/*
 * Returns a buffer of cryptographically secure pseudo-random bytes. The
 * source of bytes is nodejs' crypto.randomBytes. Sync function.
 * @param{number} howMany - the number of bytes to  return
 * @return {buffer} a InRangebuffer with "howMany" pseudo-random bytes.
 * @throws Error if insufficient entropy
 */
function nextBytes(numBytes) {
    try {
        return randomBytes(numBytes);
    }
    catch (ex) {
        throw new Error('Insufficient entropy');
    }
}
/*
 * Returns a cryptographically secure pseudo-random integer in range [min,max].
 * The source of randomness underneath is nodejs' crypto.randomBytes.
 * Sync function.
 * @param {number} min - minimum possible value of the returned integer
 * @param {number} max - maximum possible value of the returned integer
 * @return {number} - a pseudo-random integer in [min,max]
 */
function randomRange(min, max) {
    const range = (max - min);
    const bits = Math.floor(Math.log2(range)) + 1;
    // decide how many bytes we need to draw from nextBytes: drawing less
    // bytes means being more efficient
    const bytes = Math.ceil(bits / 8);
    // we use a mask as an optimization: it increases the chances for the
    // candidate to be in range
    const mask = Math.pow(2, bits) - 1;
    let candidate;
    do {
        candidate = parseInt(nextBytes(bytes).toString('hex'), 16) & mask;
    } while (candidate > range);
    return (candidate + min);
}
/**
 * This shuffles an array of any length, using sufficient entropy
 * in every single case.
 * @param {Array} array - Any type of array
 * @return {Array} - The sorted array
 */
function shuffle(array) {
    if (array.length === 1) {
        return array;
    }
    for (let i = array.length - 1; i > 0; i--) {
        const randIndex = randomRange(0, i);
        const randIndexVal = array[randIndex];
        array[randIndex], array[i] = array[i], array[randIndex]; // eslint-disable-line no-param-reassign
        array[i] = randIndexVal; // eslint-disable-line no-param-reassign
    }
    return array;
}
exports.shuffle = shuffle;
;
