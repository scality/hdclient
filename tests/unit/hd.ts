'use strict'; // eslint-disable-line strict

import * as assert from 'assert';
import * as nock from 'nock';
import * as stream from 'stream';
import { HDProxydClient, HDProxydError, HDProxydOptions } from '../../src/hdcontroller';
import { shuffle } from '../../src/shuffle';
import { idText } from 'typescript';

describe('HD Controller client', () => {
    describe('test init', () => {
        it('should be possible to instantiate something', () => {
            const h = new HDProxydClient(<HDProxydOptions>{
                bootstrap: ['1.2.3.4:12345', '1.2.3.4:1234'],
            });
            assert.ok(h, 'cannot create object');
            assert.ok(h.getCurrentBootstrap()[0] === '1.2.3.4',
            'cannot retrieve bootstrap');
        });
    });
    describe('test shuffle', () => {
        it("should be randomized", () => {
            shuffle(["a"]);
            shuffle(["a", "b", "c"]);
            shuffle(Array.from({length:100000}).map(x => "X"));
        });
    });
    describe('basic request test', () => {
        const h = new HDProxydClient(<HDProxydOptions>{
            bootstrap: ['1.2.3.4:12345'],
        });

        it('should be postable', (done) => {
            nock("http://1.2.3.4:12345")
            .post("/store/")
            .reply(200, "body", {"Scal-Key": "testing"});
            const upStream = new stream.Readable;
            upStream.push("upload");
            upStream.push(null);
            h.put(upStream, 6, {bucketName:
                "test", owner:
                "test", namespace: "nspace"}, "nope",
                (err: Error, key: string) => {
                    if (err) {
                        done(err);
                        return;
                    }
                    if (key !== 'testing') {
                        done(Error("wrong key returned"));
                        return; 
                    }
                    done(err);
                });
            });
            it("should be readable", (done) => {
                nock("http://1.2.3.4:12345")
                .get("/store/testing")
                .reply(200, "bodyReturned");
                h.get("testing", null, "", (err: Error, st: stream.Stream) => {
                    var data = '';
                    st.on("data", (chunk) => {data += chunk;})
                    st.on("end", function() {
                        assert(data === "bodyReturned", "wrong data: " + data);
                        done(err);
                    });
                });
            });
            it("should be deletable", (done) => {
                nock("http://1.2.3.4:12345")
                .delete("/store/testing")
                .reply(200, "body");
                h.delete("testing", "", (err) => {
                    done(err);
                });
            });
            it("should respond ok on healthcheck", (done) => {
                nock("http://1.2.3.4:12345")
                .get("/metrics")
                .reply(200, "body");
                h.healthcheck(undefined, done);
            })
        });
        describe('error', () => {
            it("should react properly on an error", (done) => {
                nock("http://1.2.3.4:12345")
                .get("/store/testing")
                .replyWithError(Error("test"));
                const h = new HDProxydClient(<HDProxydOptions>{
                    bootstrap: ['1.2.3.4:12345'],
                });
                h.get("testing", [], "", (err) => {
                    if (!err) {
                        done(Error("no error received"))
                    }
                    done();
                })
            });

        });
        describe('failover test', () => {
            it('should raise an error if key is not returned', (done) => {
                let received = false;
                nock("http://1.2.3.4:12345")
                .post("/store/")
                .reply(500, () => { received = true;});
                nock("http://1.2.3.5:12345")
                .post("/store/")
                .reply(200, "body", {"Scal-Key": "testing"});
                const h = new HDProxydClient(<HDProxydOptions>{
                    bootstrap: ['1.2.3.4:12345', '1.2.3.5:12345'],
                });
                h.bootstrap = [['1.2.3.4', '12345'], ['1.2.3.5', '12345']]
                h.setCurrentBootstrap(h.bootstrap[0]);
                const upStream = new stream.Readable;
                upStream.push("upload");
                upStream.push(null);
                h.put(upStream, 6, {bucketName:
                    "test", owner:
                    "test", namespace: "nspace"}, "nope",
                    (err: HDProxydError, key: string) => {
                        if (key !== "testing") {
                            done(Error("wrong key or no key received"));
                            return;
                        }
                        if (received === false) {
                            done(Error("first server did not receive anything"));
                            return;
                        }
                        done(err);
                    });
                });
            });
        describe('wrong key returned', () => {
            it('should raise an error if key is not returned', (done) => {
                nock("http://1.2.3.4:12345")
                .post("/store/")
                .reply(200, "body", {"Scal-NotKey": "testing"});
                const h = new HDProxydClient(<HDProxydOptions>{
                    bootstrap: ['1.2.3.4:12345'],
                });
                const upStream = new stream.Readable;
                upStream.push("upload");
                upStream.push(null);
                h.put(upStream, 6, {bucketName:
                    "test", owner:
                    "test", namespace: "nspace"}, "nope",
                    (err: HDProxydError, key: string) => {
                        if (!err) {
                            done(err);
                            return;
                        }
                        done();
                    });
                });
            });
        describe('404 test', () => {
            it('should NOT be POSTed', (done) => {
                nock("http://1.2.3.4:12345")
                .post("/store/")
                .reply(404, "body", {"Scal-Key": "testing"});
                const h = new HDProxydClient(<HDProxydOptions>{
                    bootstrap: ['1.2.3.4:12345'],
                });
                const upStream = new stream.Readable;
                upStream.push("upload");
                upStream.push(null);
                h.put(upStream, 6, {bucketName:
                    "test", owner:
                    "test", namespace: "nspace"}, "nope",
                    (err: HDProxydError, key: string) => {
                        if (!err) {
                            done(Error("expected error"));
                            return;
                        }
                        if (err.code != 404) {
                            done(Error("wrong code returned"));
                            return; 
                        }
                        done();
                    });
                });
                it("should NOT be readable", (done) => {
                    nock("http://1.2.3.4:12345")
                    .get("/store/testing")
                    .reply(404, "bodyReturned");
                    const h = new HDProxydClient(<HDProxydOptions>{
                        bootstrap: ['1.2.3.4:12345'],
                    });  
                    h.get("testing", null, "", (err: HDProxydError, st: stream.Stream) => {
                        if (!err) {
                            done(Error("expected error"));
                            return;
                        }
                        if (err.code != 404) {
                            done(Error("wrong code returned " + err));
                            return; 
                        }
                        done();
                    });
                });
                it("should NOT be deletable", (done) => {
                    nock("http://1.2.3.4:12345")
                    .delete("/store/testing")
                    .reply(404, "body");
                    const h = new HDProxydClient(<HDProxydOptions>{
                        bootstrap: ['1.2.3.4:12345'],
                    });  
                    h.delete("testing", "", (err: HDProxydError) => {
                        if (!err) {
                            done(Error("expected error"));
                            return;
                        }
                        if (err.code != 404) {
                            done(Error("wrong code returned"));
                            return; 
                        }
                        done();
                    });
                });
            });
    });