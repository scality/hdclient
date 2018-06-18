'use strict'; // eslint-disable-line strict
/* eslint-disable prefer-arrow-callback */ // Mocha recommends not using => func
/* eslint-disable func-names */

const mocha = require('mocha');
const assert = require('assert');
const http = require('http');
const nock = require('nock'); // HTTP API mocking

mocha.describe('HTTP Mockup basic example', function () {
    mocha.describe('#GET', function () {
        const getPayload = 'je suis une mite en pull over';
        mocha.beforeEach(function () {
            nock('http://127.0.0.1:7777')
                .get('/store/random_key')
                .reply(200, getPayload);
        });

        mocha.it('do get', function (done) {
            // Perform fake HTTP call
            const replyPromise = new Promise(function (resolve, reject) {
                // Do async job
                const replyCb = function (resp) {
                    let data = '';
                    // A chunk of data has been recieved.
                    resp.on('data', chunk => {
                        data += chunk;
                    });

                    // The whole response has been received.
                    // Print out the result.
                    resp.on('end', () => {
                        resolve({ resp, data });
                    });
                };

                http.get('http://127.0.0.1:7777/store/random_key',
                         replyCb)
                    .on('error', err => {
                        reject(err);
                    });
            });

            const resultCb = function (result) {
                assert.strictEqual(result.resp.statusCode, 200);
                assert.strictEqual(getPayload, result.data,
                                   'Unexpected payload');
                done();
            };
            const errorCb = function (error) {
                done(error);
            };

            replyPromise
                .then(resultCb, errorCb)
                .catch(function (err) {
                    done(err);
                });
            // don't return the Promise, Mocha does not handle it properly...
        });
    });
});
