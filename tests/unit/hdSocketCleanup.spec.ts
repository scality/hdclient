'use strict';

/**
 * Regression tests: after the HTTP response callback has run, Node may still emit
 * ClientRequest 'error' (e.g. ERR_SOCKET_TIMEOUT on keep-alive / agent sockets).
 * HDProxydClient must call request.destroy() so FDs do not accumulate under retries.
 *
 */

import * as assert from 'assert';
import * as sinon from 'sinon';
import * as stream from 'stream';
import * as http from 'http';
import { AddressInfo } from 'net';
import { HDProxydClient, HDProxydOptions } from '../../src/hdcontroller';

describe('HDProxydClient — outbound socket cleanup after response', () => {
    let sandbox: sinon.SinonSandbox;
    let server: http.Server;

    beforeEach(() => {
        sandbox = sinon.createSandbox();
        server = http.createServer((req, res) => {
            if (req.url?.startsWith('/store/')) {
                res.writeHead(200, { 'scal-key': 'test-key' });
                res.end('ok');
                return;
            }
            // eslint-disable-next-line no-param-reassign
            res.statusCode = 404;
            res.end();
        });
    });

    afterEach(done => {
        sandbox.restore();
        const s = server as http.Server & { closeAllConnections?: () => void };
        if (typeof s.closeAllConnections === 'function') {
            s.closeAllConnections();
        }
        server.close(() => done());
    });

    function listenServer(cb: (port: number) => void): void {
        server.listen(0, '127.0.0.1', () => {
            const addr = server.address() as AddressInfo;
            cb(addr.port);
        });
    }

    type DestroyCapture = {
        destroyCalled: boolean;
        destroyCallCount: number;
        lastDestroyArg?: unknown;
        firstDestroyArgFromStream?: unknown;
    };


    function stubRequestAndCaptureDestroy(options: {
        assignCapture: (cap: DestroyCapture) => void;
        postResponseError?: {
            code: 'ERR_SOCKET_TIMEOUT' | 'ECONNRESET';
            message: string;
        };
    }): void {
        const origRequest = http.request;
        sandbox.stub(http, 'request').callsFake(((opts, cb) => {
            const req = origRequest(
                opts as string | http.RequestOptions,
                cb as (res: http.IncomingMessage) => void,
            );
            const destroyCapture: DestroyCapture = { destroyCalled: false, destroyCallCount: 0 };
            options.assignCapture(destroyCapture);
            const innerDestroy = req.destroy.bind(req);
            req.destroy = (error?: Error) => {
                destroyCapture.destroyCalled = true;
                destroyCapture.destroyCallCount += 1;
                destroyCapture.lastDestroyArg = error;
                if (destroyCapture.firstDestroyArgFromStream === undefined) {
                    destroyCapture.firstDestroyArgFromStream = error;
                }
                return innerDestroy();
            };
            if (options.postResponseError) {
                req.once('response', () => {
                    process.nextTick(() => {
                        const err = new Error(options.postResponseError!.message) as NodeJS.ErrnoException;
                        err.code = options.postResponseError!.code;
                        req.emit('error', err);
                    });
                });
            }
            return req;
        }) as typeof http.request);
    }

    function countAgentSockets(agent: unknown): number {
        const a = agent as {
            sockets?: Record<string, unknown>;
            freeSockets?: Record<string, unknown>;
        };
        const countPool = (pool?: Record<string, unknown>) => {
            if (!pool) {
                return 0;
            }
            return (Object.values(pool) as unknown[]).reduce<number>((sum, entry) => {
                if (Array.isArray(entry)) {
                    return sum + entry.length;
                }
                return sum + 1;
            }, 0);
        };
        return countPool(a.sockets) + countPool(a.freeSockets);
    }

    it('calls ClientRequest.destroy when ERR_SOCKET_TIMEOUT fires after response', done => {
        let destroyCapture: DestroyCapture | undefined;
        let finished = false;
        const finish = (e?: Error) => {
            if (finished) {return;}
            finished = true;
            done(e);
        };
        stubRequestAndCaptureDestroy({
            postResponseError: {
                code: 'ERR_SOCKET_TIMEOUT',
                message: 'simulated idle timeout',
            },
            assignCapture: (capture: DestroyCapture) => { destroyCapture = capture; },
        });

        listenServer(port => {
            const h = new HDProxydClient({
                bootstrap: [`127.0.0.1:${port}`],
            } as HDProxydOptions);
            h.get('anykey', [], '', (err, st) => {
                assert.ifError(err);
                assert.ok(st);
                const body = st as stream.Readable;
                body.on('error', () => {});
                body.resume();
                setImmediate(() => {
                    try {
                        assert.ok(destroyCapture!, 'capture should be set');
                        assert.ok(destroyCapture!.destroyCalled, 'request.destroy must run for post-response socket error');
                        assert.strictEqual(destroyCapture!.destroyCallCount, 1, 'request.destroy should run once');
                        const arg0 = destroyCapture!.lastDestroyArg as NodeJS.ErrnoException;
                        assert.strictEqual(arg0?.code, 'ERR_SOCKET_TIMEOUT');
                        h.destroy();
                        finish();
                    } catch (e) {
                        h.destroy();
                        finish(e as Error);
                    }
                });
            });
        });
    });

    it('calls ClientRequest.destroy when a non-timeout error fires after response', done => {
        let destroyCapture: DestroyCapture | undefined;
        let finished = false;
        const finish = (e?: Error) => {
            if (finished) { return; }
            finished = true;
            done(e);
        };
        stubRequestAndCaptureDestroy({
            postResponseError: {
                code: 'ECONNRESET',
                message: 'simulated reset',
            },
            assignCapture: (capture: DestroyCapture) => { destroyCapture = capture; },
        });

        listenServer(port => {
            const h = new HDProxydClient({
                bootstrap: [`127.0.0.1:${port}`],
            } as HDProxydOptions);
            h.get('anykey', [], '', (err, st) => {
                assert.ifError(err);
                assert.ok(st);
                const body = st as stream.Readable;
                body.on('error', () => {});
                body.resume();
                setImmediate(() => {
                    try {
                        assert.ok(destroyCapture, 'capture should be set');
                        assert.ok(destroyCapture!.destroyCalled, 'request.destroy must run');
                        assert.strictEqual(destroyCapture!.destroyCallCount, 1, 'request.destroy should run once');
                        const arg0 = destroyCapture!.lastDestroyArg as NodeJS.ErrnoException;
                        assert.strictEqual(arg0?.code, 'ECONNRESET');
                        h.destroy();
                        finish();
                    } catch (e) {
                        h.destroy();
                        finish(e as Error);
                    }
                });
            });
        });
    });

    it('calls ClientRequest.destroy when upload stream errors mid-transfer (PUT stream path)', done => {
        let destroyCapture: DestroyCapture | undefined;
        let finished = false;
        const finish = (e?: Error) => {
            if (finished) {return;}
            finished = true;
            done(e);
        };
        const injectedError = new Error('simulated upload stream failure');
        stubRequestAndCaptureDestroy({
            assignCapture: (capture: DestroyCapture) => { destroyCapture = capture; },
        });

        listenServer(port => {
            const h = new HDProxydClient({
                bootstrap: [`127.0.0.1:${port}`],
            } as HDProxydOptions);
            const upload = new stream.Readable({
                read() {
                    this.push(Buffer.from('hello'));
                    this.destroy(injectedError);
                },
            });
            h.put(upload, 5, {
                bucketName: 'test',
                owner: 'test',
                namespace: 'zenko',
            }, '', () => {
                setImmediate(() => {
                    try {
                        assert.ok(destroyCapture, 'capture should be set');
                        assert.ok(destroyCapture!.destroyCalled, 'request.destroy must run for upload stream error');
                        // Node may issue a second destroy from its internal pipe error handling; 
                        // we are allowing both while requiring our stream error to trigger first.
                        assert.ok(
                            destroyCapture!.destroyCallCount === 1 || destroyCapture!.destroyCallCount === 2,
                            `request.destroy should run once (or twice at most if http request also errors); 
                            callCount=${destroyCapture!.destroyCallCount}`,
                        );
                        assert.strictEqual(destroyCapture!.firstDestroyArgFromStream, injectedError);
                        h.destroy();
                        finish();
                    } catch (e) {
                        h.destroy();
                        finish(e as Error);
                    }
                });
            });
        });
    });

    it('does not accumulate agent sockets across repeated post-response ECONNRESET errors', done => {
        let finished = false;
        const finish = (e?: Error) => {
            if (finished) {return;}
            finished = true;
            done(e);
        };
        stubRequestAndCaptureDestroy({
            postResponseError: {
                code: 'ECONNRESET',
                message: 'simulated reset for leak regression',
            },
            assignCapture: () => {},
        });

        listenServer(port => {
            const h = new HDProxydClient({
                bootstrap: [`127.0.0.1:${port}`],
            } as HDProxydOptions);
            const baselineSockets = countAgentSockets((h as unknown as { httpAgent: unknown }).httpAgent);
            let remaining = 20;

            const runOnce = () => {
                h.get('anykey', [], '', (err, st) => {
                    if (err) {
                        h.destroy();
                        return finish(err);
                    }
                    assert.ok(st);
                    const body = st as stream.Readable;
                    body.on('error', () => {});
                    body.resume();
                    return setImmediate(() => {
                        remaining -= 1;
                        if (remaining === 0) {
                            const afterSockets = countAgentSockets((h as unknown as { httpAgent: unknown }).httpAgent);
                            try {
                                assert.ok(
                                    afterSockets <= baselineSockets + 2,
                                    `socket count should stay bounded (baseline=${baselineSockets}, 
                                    after=${afterSockets})`,
                                );
                                h.destroy();
                                return finish();
                            } catch (e) {
                                h.destroy();
                                return finish(e as Error);
                            }
                        }
                        return runOnce();
                    });
                });
            };
            runOnce();
        });
    });
});
