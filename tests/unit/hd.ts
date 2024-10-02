'use strict';

import * as assert from 'assert';
import * as sinon from 'sinon';
import * as nock from 'nock';
import * as stream from 'stream';
import * as http from 'http';
import { HDProxydClient, HDProxydError, HDProxydOptions } from '../../src/hdcontroller';
import { shuffle } from '../../src/shuffle';
import {setTimeout} from 'timers/promises';

describe('HD Controller client', () => {
    describe('test init', () => {
        it('should be possible to instantiate something', () => {
            const h = new HDProxydClient({
                bootstrap: ['1.2.3.4:12345', '1.2.3.4:1234'],
            } as HDProxydOptions);
            assert.ok(h, 'cannot create object');
            assert.ok(h.getCurrentBootstrap()[0] === '1.2.3.4',
                'cannot retrieve bootstrap');
        });
    });
    describe('test shuffle', () => {
        it('should be randomized', () => {
            shuffle(['a']);
            shuffle(['a', 'b', 'c']);
            shuffle(Array.from({length: 100000}).fill('X'));
        });
    });
    describe('basic request test', () => {
        const h = new HDProxydClient({
            bootstrap: ['1.2.3.4:12345'],
        } as HDProxydOptions);

        it('should be postable', done => {
            nock('http://1.2.3.4:12345')
                .post('/store/')
                .reply(200, 'body', {'Scal-Key': 'testing'});
            const upStream = new stream.Readable();
            upStream.push('upload');
            upStream.push(null);
            h.put(upStream, 6, {
                bucketName: 'test',
                owner: 'test',
                // tslint:disable-next-line: object-literal-sort-keys
                namespace: 'nspace'}, 'nope',
            (err: HDProxydError | undefined, key: string | undefined) => {
                if (err) {
                    done(err);
                    return;
                }
                if (key !== 'testing') {
                    done(Error('wrong key returned'));
                    return;
                }
                done(err);
            });
        });
        it('should be readable', done => {
            nock('http://1.2.3.4:12345')
                .get('/store/testing')
                .reply(200, 'bodyReturned');
            h.get('testing', [], '', (err: HDProxydError | undefined, st: stream.Stream | undefined) => {
                let data = '';
                st?.on('data', chunk => {data += chunk; });
                st?.on('end', () => {
                    assert(data === 'bodyReturned', `wrong data: ${  data}`);
                    done(err);
                });
            });
        });
        it('should be deletable', done => {
            nock('http://1.2.3.4:12345')
                .delete('/store/testing')
                .reply(200, 'body');
            h.delete('testing', '', err => {
                done(err);
            });
        });
        it('should respond ok on healthcheck', done => {
            nock('http://1.2.3.4:12345')
                .get('/metrics')
                .reply(200, 'body');
            h.healthcheck(undefined, done);
        });
        it('should be batch deletable', done => {
            nock('http://1.2.3.4:12345')
                .post('/job/delete', ['testing']) // Will verify that the body is ok
                .reply(200, 'body');
            h.batchDelete({keys: ['testing']}, '', err => {
                if (err) {
                    done(Error(`unexpected error ${  err}`));
                    return;
                }
                done();
            });
        });
    });
    describe('error', () => {
        it('should react properly on an error', done => {
            nock('http://1.2.3.4:12345')
                .get('/store/testing')
                .replyWithError(Error('test'));
            const h = new HDProxydClient({
                bootstrap: ['1.2.3.4:12345'],
            } as HDProxydOptions);
            h.get('testing', [], '', err => {
                if (!err) {
                    done(Error('no error received'));
                }
                done();
            });
        });

        it('should drain the response pipe in case of error', done => {
            nock('http://1.2.3.4:12345')
                .post('/store/')
                .reply(500, () => {
                    const upStream = new stream.Readable();
                    upStream.push('upload');
                    upStream.push(null);
                    //upStream.on("close", () => {received = true;});
                    return upStream;
                });
            const upStream = new stream.Readable();
            upStream.push('upload');
            upStream.push(null);

            const h = new HDProxydClient({
                bootstrap: ['1.2.3.4:12345'],
            } as HDProxydOptions);

            // Let's replace http.request by a new capturing CB
            let received: boolean = false;
            const prev = http.request;
            sinon.replace(http, 'request', (v, cb) => prev(v, response => {
                response.on('end', () => {received = true;});
                return cb(response);
            }));

            h.put(upStream, 6, {
                bucketName: 'test',
                owner: 'test',
                // tslint:disable-next-line: object-literal-sort-keys
                namespace: 'nspace'}, 'nope',
            async (err: HDProxydError|undefined) => {
                // Remove fake http.request
                sinon.restore();
                if (!err) {
                    done(Error('error expected, got success'));
                    return;
                }

                // Let's give it a few scheduler runs
                // to make events are consumed
                await setTimeout(100);
                if (!received) {
                    done(Error('stream was not consumed'));
                    return;
                }
                done();
            });
        });
    });
    describe('failover test', () => {
        it('should raise an error if key is not returned', done => {
            let received = false;
            nock('http://1.2.3.4:12345')
                .post('/store/')
                .reply(500, () => { received = true; });
            nock('http://1.2.3.5:12345')
                .post('/store/')
                .reply(200, 'body', {'Scal-Key': 'testing'});
            const h = new HDProxydClient({
                bootstrap: ['1.2.3.4:12345', '1.2.3.5:12345'],
            } as HDProxydOptions);
            h.bootstrap = [['1.2.3.4', '12345'], ['1.2.3.5', '12345']];
            h.setCurrentBootstrap(h.bootstrap[0]);
            const upStream = new stream.Readable();
            upStream.push('upload');
            upStream.push(null);
            h.put(upStream, 6, {bucketName:
                    'test', owner:
                    // tslint:disable-next-line: object-literal-sort-keys
                    'test', namespace: 'nspace'}, 'nope',
            (err , key) => {
                if (key !== 'testing') {
                    done(Error('wrong key or no key received'));
                    return;
                }
                if (received === false) {
                    done(Error('first server did not receive anything'));
                    return;
                }
                done(err);
            });
        });
    });
    describe('wrong key returned', () => {
        it('should raise an error if key is not returned', done => {
            nock('http://1.2.3.4:12345')
                .post('/store/')
                .reply(200, 'body', {'Scal-NotKey': 'testing'});
            const h = new HDProxydClient({
                bootstrap: ['1.2.3.4:12345'],
            } as HDProxydOptions);
            const upStream = new stream.Readable();
            upStream.push('upload');
            upStream.push(null);
            h.put(upStream, 6, {
                bucketName: 'test',
                owner: 'test',
                // tslint:disable-next-line: object-literal-sort-keys
                namespace: 'nspace'}, 'nope', err => {
                if (!err) {
                    done(err);
                    return;
                }
                done();
            });
        });
    });
    describe('404 test', () => {
        it('should NOT be POSTed', done => {
            nock('http://1.2.3.4:12345')
                .post('/store/')
                .reply(404, 'body', {'Scal-Key': 'testing'});
            const h = new HDProxydClient({
                bootstrap: ['1.2.3.4:12345'],
            } as HDProxydOptions);
            const upStream = new stream.Readable();
            upStream.push('upload');
            upStream.push(null);
            h.put(upStream, 6, {
                bucketName: 'test',
                owner: 'test',
                // tslint:disable-next-line: object-literal-sort-keys
                namespace: 'nspace'}, 'nope', err  => {
                if (!err) {
                    done(Error('expected error'));
                    return;
                }
                if (err.code !== 404) {
                    done(Error('wrong code returned'));
                    return;
                }
                done();
            });
        });
        it('should NOT be readable', done => {
            nock('http://1.2.3.4:12345')
                .get('/store/testing')
                .reply(404, 'bodyReturned');
            const h = new HDProxydClient({
                bootstrap: ['1.2.3.4:12345'],
            } as HDProxydOptions);
            h.get('testing', [], '', err => {
                if (!err) {
                    done(Error('expected error'));
                    return;
                }
                if (err.code !== 404) {
                    done(Error(`wrong code returned ${ err }`));
                    return;
                }
                done();
            });
        });
        it('should NOT be deletable', done => {
            nock('http://1.2.3.4:12345')
                .delete('/store/testing')
                .reply(404, 'body');
            const h = new HDProxydClient({
                bootstrap: ['1.2.3.4:12345'],
            } as HDProxydOptions);
            h.delete('testing', '', err => {
                if (!err) {
                    done(Error('expected error'));
                    return;
                }
                if (err.code !== 404) {
                    done(Error('wrong code returned'));
                    return;
                }
                done();
            });
        });
    });
});
