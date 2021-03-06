/* eslint-disable strict */
/* eslint-disable prefer-destructuring */
/* eslint-disable no-bitwise */
/* eslint-disable no-console */

'use strict';

/**
 * Mini server used to quickly test Hyperdrive client
 * against a real hyperdrive, without going full S3.
 *
 * Idea is to provide a HTTP proxy so that you can
 * go through hdclient with curl and the likes
 *
 * Example:
 * curl -XGET http://host:port/mybucket/myboject
 */

import { createServer } from 'http';
import { readFileSync } from 'fs';

import { hdcontroller } from '../index';

/* On DELETE, should we remove the object from the
 * in-memory index?
 *
 * if false, GET and DELETE will still be forwarded
 * to the hyperdrives (and returning 404 instead of 500)
*/
let removeMemIndexOnDelete = true;

function getHyperdriveClient(config) {
    return new hdcontroller.HDProxydClient(config);
}

/**
 * Main HTTP server callback
 *
 * @param {client.hdclient.HyperdriveClient} client to use
 * @param {Map} objectMap to store in-memory obj -> rawkey mapping
 * @param {http.IncomingMessage} request to serve
 * @param {http.Serverresponse} response handler
 * @return {undefined}
 */
function serverCallback(client, objectMap, request, response) {
    try {
        switch (request.method) {
        case 'GET':
        {
            const rawKey = objectMap.get(request.url);
            if (!rawKey) {
                response.writeHead(404);
                response.end();
            } else {
                const requestedRange = request.headers.range;
                let range = null;
                if (requestedRange) {
                    range = requestedRange.split('-').map(n => ~~n);
                }
                client.get(
                    rawKey, range, '1',
                    (err, reply) => {
                        if (err) {
                            response.writeHead(
                                err.infos ? err.infos.status : 500,
                            );
                            response.end();
                            return;
                        }

                        const clen = reply.headers['content-length'];
                        response.writeHead(200,
                            {
                                'Content-Length': clen,
                            });
                        /* Forward output and chain errors */
                        reply.on('error',
                            err => response.emit('error', err));
                        reply.pipe(response);
                    },
                );
            }
            break;
        }

        case 'HEAD':
        {
            throw Error(`Unhandled HTTP method ${request.method}`);
        }

        case 'PUT':
        {
            const size = request.headers['content-length'];
            const [, bucketName, objectKey, version] = request.url.split('/');
            client.put(
                request, size, { bucketName, objectKey, version }, '1',
                (err, genkey) => {
                    if (err) {
                        response.writeHead(
                            err.infos ? err.infos.status : 500,
                        );
                        response.end();
                        return;
                    }
                    objectMap.set(request.url, genkey);
                    response.writeHead(200);
                    response.write(`key for ${request.url} is ${genkey}`);
                    response.end();
                },
            );
            break;
        }

        case 'DELETE':
        {
            const rawKey = objectMap.get(request.url);
            if (rawKey === null) {
                response.writeHead(404);
                response.end();
            } else {
                client.delete(
                    rawKey, '1', err => {
                        if (err) {
                            response.writeHead(
                                err.infos ? err.infos.status : 500,
                            );
                        } else {
                            if (removeMemIndexOnDelete) {
                                objectMap.delete(request.url);
                            }
                            response.writeHead(200);
                        }
                        response.end();
                    },
                );
            }
            break;
        }

        default:
        {
            throw Error(`Unhandled HTTP method ${request.method}`);
        }
        }
    } catch (err) {
        response.writeHead(500);
        response.write(err.message);
        response.end();
    }
}

/**
 * Load Hyperdrive client JSON config file
 * @param {String} file path to config
 * @return {Object} JSON parsed conf
 */
function loadConfig(file) {
    return JSON.parse(readFileSync(file));
}


function main() {
    const args = process.argv;
    if (args.length < 4) {
        console.error(`Usage: <port> <conf path> <memindexnodel>
{Number} port to listen on
{String} path to HyperdriveClient json config
{*}      don't remove in-memory keys on DELETE (used to check
         behavior of GET/DELETE hyperdrive 404
`);
        process.exit(1);
    }

    if (args.length === 5 && !!args[4]) {
        removeMemIndexOnDelete = false;
    }

    const port = Number(args[2]);
    const config = loadConfig(args[3]);

    const client = getHyperdriveClient(config);
    const object2rawkey = new Map();
    const server = createServer(
        (req, res) => serverCallback(client, object2rawkey, req, res),
    );
    server.listen(port, () => console.log('Listening on %d',
        server.address().port));
    server.on('connection', socket => socket.setNoDelay(true));
}

/* If run as a script */
if (typeof require !== 'undefined' && require.main === module) {
    main();
}
