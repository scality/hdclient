'use strict'; // eslint-disable-line strict

const stream = require('stream');


/** BIZOP protocol shenanigan
 *
 * Hyperdrive does not bufferize everything before sending,
 * so in order to notify client some thing is corrupted, it is
 * using a custom form of trailings.
 * Adding $crc in GET Accept header, hyperdrive returns
 * $crc.data in Content-Type. This filed contains CRC computed
 * at PUT time (and stored in its index).
 * The body of the reply contains a concatenation of actual data
 * and final 12 added bytes: 3 * 4 binary dump of computed CRCs.
 * Corruption is detected whenever trailingCRC does not match
 * $crc.data
 *
 * Trailing CRC layout: data(0-3 bytes), meta(4-7) and usermd(8-11)
 * Every CRC is an unsigned lttle-endian integer.
 */
class CorruptSafeStream extends stream.Transform {
    /**
     * Get a new corruption-proof stream
     *
     * @constructor
     * @param {Object} reqContext - Corresponding fragment context
     * @param {Object} errorAgent - Error handler
     * @param {Number} size - Stream length
     * @param {Number} expectedCRC - Expected stream checksum
     * @param {Object} options - passed to parent Transform (refer to it)
     */
    constructor(reqContext, errorAgent, size, expectedCRC, options) {
        super(options);
        this.endDataBuffers = [];
        this.crcBuffers = [];
        this.readCRCbytes = 0;
        this.bytesUntilCRC = size;
        this.reqContext = reqContext;
        this.errorAgent = errorAgent;
        this.size = size;
        this.expectedCRC = expectedCRC;
    }

    /**
     * Log and persist corruption
     *
     * @param {Number} actualCRC - Computed data CRC
     * @return {undefined}
     */
    _handleCorruption(actualCRC) {
        const opContext = this.reqContext.opContext;
        opContext.log.error(
            'Corrupted data',
            { expectedCRC: this.expectedCRC, actualCRC });

        const corruptedError = new Error('Corrupted data');
        corruptedError.infos = {
            status: 422,
            method: 'GET',
        };

        const chunkStatus = opContext.status[this.reqContext.chunkId];
        const fragmentStatus = chunkStatus.statuses[this.reqContext.fragmentId];
        chunkStatus.nOk--;
        chunkStatus.nError++;
        fragmentStatus.reply = null; // resset it
        fragmentStatus.error = corruptedError;

        return corruptedError;
    }

    /**
     * Extracts trailing CRCs from data stream
     * and check integrity
     *
     * @param {Buffer} chunk - Piece of data to process
     * @param {String} encoding - Encoding used (should normally be binary)
     * @param {Function} continueCb - Tells transformer to continue,
     *                      or emit error: continueCb(null|undefined|Error)
     * @return {undefined}
     */
    _transform(chunk, encoding, continueCb) {
        /* Simple forward until we reach end of
         * data/beginning of CRCs */
        if (chunk.length < this.bytesUntilCRC) {
            this.push(chunk);
            this.bytesUntilCRC -= chunk.length;
            continueCb();
            return;
        }

        /* We have reached the end of the data, hold onto
         * the last piece until we have fully read and
         * checked the CRC. Otherwise it will be too late
         * to notify upper layers of the corruption.
         */
        this.endDataBuffers.push(chunk.slice(0, this.bytesUntilCRC));
        this.crcBuffers.push(chunk.slice(this.bytesUntilCRC));
        this.readCRCbytes += this.crcBuffers[this.crcBuffers.length - 1].length;

        /* Partial CRC - wait for the rest */
        if (this.readCRCbytes < 12) {
            this.bytesUntilCRC -= chunk.length;
            continueCb();
            return;
        }

        /* We have everything - validate data CRC
         * First 4 bytes are data crc */
        const crcBuffer = Buffer.concat(this.crcBuffers);
        const actualCRC = crcBuffer.slice(0, 4).readUInt32LE();
        if (actualCRC !== this.expectedCRC) {
            const error = this._handleCorruption(actualCRC);
            continueCb(error);
            return;
        }

        this.endDataBuffers.forEach(chunk => this.push(chunk));
        this.bytesUntilCRC -= chunk.lengh;
        continueCb();
    }
}

module.exports = {
    CorruptSafeStream,
};
