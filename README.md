# NodeJS hyperdrive client API

## Contributing

In order to contribute, please follow the
[Contributing Guidelines](
https://github.com/scality/Guidelines/blob/master/CONTRIBUTING.md).

## Overview

The hyperdrive client is not strictly managing a single hyperdrive endpoint.
Its mission is export an API showing a unified, single namespace over
several servers and hyperdrives, managing network-level erasure coding and
large payload splitting.

### Hyperdrive protocol
Basically a subset of HTTP 1.1 with extentded, custom headers and
data layout. Protocol was created to work under Scality RING software where
each objectid can be assigned three blob payloads:
* data: the regular payload
* usermetadata: a small, on-the-side description about the data
* metadata: super small (<64 bytes), used internally by RING software.

At this phase of the design we don't know yet whether the three kinds of
payload will be put to use. When putting an object, the several payloads
are simply concatenated, the layout being described in a special header.

On get, the protocol returns the requested payloads of a given key, concatenated
with a format described again in the HTTP headers. Special mention is handling of
CRCs: stored CRC is returned as part of the headers. The actual CRC of returned data
is a special, fourth kind of payload appended to the others, and is typically used
to detect data corruption.

Full specification can be found [here](https://docs.scality.com/display/STORAGE/BIZOP.1:+bizio+HTTP+protocol+specifications).
Link above provides an in-depth but slightly outdated description. Changes introdcued by hyperdrive itself
can be found in [hyperdrive project README](https://bitbucket.org/scality/ring/src/f0a3b504523bc0049f8892b1530f3209d0436a1c/?at=feature%2FRING-21232-hyperdrive).

Hyperdrive client functional requirements and specs are inside the [main Zenko architecture document](https://docs.google.com/document/d/1XdOn7MKuWLGb6hd9Apa3Q54uvO5K7InURnskvYH8WVw).

## Usage

### Installation

```shell
npm install --save scality/hdclient
```

### Running tests
To use the linter and run the tests:

```shell
# ESLint with Scality's custom rules
npm run lint

# All tests
npm test tests/*

# Unit tests
npm test tests/unit/

# Functional tests
npm test tests/functional/

# Other options and Mocha help
npm test -- -h

```
