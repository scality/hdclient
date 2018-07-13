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

# Code coverage
npm run coverage tests/*

# Other options and Mocha help
npm test -- -h

```

### Running as standalone
Because deploying the full S3 server might be too much of a hassle for your specific need, a HTTP server using HdClient is provided. The mapping between object key and internal keys (the ones actually stored on the hyperdrive) is stored in-memory only. The internal keys are not accessible on the outside, mirrorring behavior of Zenko-like deployment.

 ```shell
# Start Hyperdrive 'proxy'
# example conf assumes 1 hyperdrive listening on localhost:7777
NODE_ENV=production node scripts/server.js 8888 scripts/example_hdclient_proxy.conf.json &

# Have fun
curl -XPUT --data @/etc/hosts -v http://localhost:8888/mybucket/testobj
*   Trying 127.0.0.1...
* Connected to localhost (127.0.0.1) port 8888 (#0)
> PUT /mybucket/testobj HTTP/1.1
> Host: localhost:8888
> User-Agent: curl/7.47.0
> Accept: */*
> Content-Length: 267
> Content-Type: application/x-www-form-urlencoded
>
* upload completely sent off: 267 out of 267 bytes
< HTTP/1.1 200 OK
< Date: Wed, 27 Jun 2018 10:44:32 GMT
< Connection: keep-alive
< Transfer-Encoding: chunked
<
* Connection #0 to host localhost left intact

curl -v http://localhost:8888/mybucket/testobj
*   Trying 127.0.0.1...
* Connected to localhost (127.0.0.1) port 8888 (#0)
> GET /mybucket/testobj HTTP/1.1
> Host: localhost:8888
> User-Agent: curl/7.47.0
> Accept: */*
>
< HTTP/1.1 200 OK
< Content-Length: 267
< Date: Wed, 27 Jun 2018 10:44:39 GMT
< Connection: keep-alive
<
* Connection #0 to host localhost left intact
<payload...>

curl -XDELETE -v http://localhost:8888/mybucket/testobj
*   Trying 127.0.0.1...
* Connected to localhost (127.0.0.1) port 8888 (#0)
> DELETE /mybucket/testobj HTTP/1.1
> Host: localhost:8888
> User-Agent: curl/7.47.0
> Accept: */*
>
< HTTP/1.1 200 OK
< Date: Wed, 27 Jun 2018 10:44:58 GMT
< Connection: keep-alive
< Transfer-Encoding: chunked
<
* Connection #0 to host localhost left intact
```

### Running as a CloudServer data backend
How to run integrated hyperdrive client inside S3 or Zenko deployment? This section is only a work in progress since actual S3 integration code is not yet merged.
```shell
# Checkout S3 repository and checkout proper hdclient integration branch
git clone https://github.com/scality/S3.git
cd S3/
git checkout feature/RING-28500-add-hyperdrive-client-data-backend

# Modify package.json to use the version of hdclient you want
# To use a local repository
# sed s%scality/hdclient%file:<path to hdclient repository% package.json
# To use a tag or commit
sed -i s%scality/hdclient%scality/hdclient#<tag/commit>% package.json

# Add new locationConstraints
# Region us-east-1 is mandatory, since the default config still references it
hyperdrive_ipport="127.0.0.1:7777"
cat <<EOF > hdclient_locationConfig.json
{
    "us-east-1": {
        "type": "file",
        "legacyAwsBehavior": true,
        "details": {}
    },
    "hyperdrive-cluster-1": {
        "type": "scality",
        "legacyAwsBehavior": true,
        "details": {
            "connector": {
                "hdclient" : {
                    "policy": {
                        "locations": ["${hyperdrive_ipport}"]
                    },
                    "dataParts": 1,
                    "codingParts": 0,
                    "requestTimeoutMs": 30000
                }
            }
        }
    }
}
EOF

# Pattern match restEndpoints - haven't found a better way yet...
# Edit config.json restEndpoints section to use hyperdrive-cluster-1
# e.g. to map localhost onto hdclient: sed -i %"localhost": "us-east-1"%"localhost": "hyperdrive-cluster-1"
# e.g. to map 127.0.0.1 onto hdclient: sed -i %"127.0.0.1": "us-east-1"%"127.0.0.1": "hyperdrive-cluster-1"

# Install dependencies
npm install

# Start CloudServer (memory backend ie metadata in-memory)
# More informations inside S3 repository documentation
S3DATA=multiple S3_LOCATION_FILE=hdclient_locationConfig.json npm run mem_backend
```

In a separate tab, have fun with AWS CLI
```
# Running S3 server uses default accessKey and secretKey
export AWS_ACCESS_KEY_ID=accessKey1
export AWS_SECRET_ACCESS_KEY=verySecretKey1

# Create a bucket
aws  --endpoint-url=http://localhost:8000 s3 mb s3://brandnewbucket

# List buckets
aws  --endpoint-url=http://localhost:8000 s3 ls

# Put data
aws  --endpoint-url=http://localhost:8000 s3 cp /etc/hosts s3://brandnewbucket/shiny_new_object

# List bucket content
aws  --endpoint-url=http://localhost:8000 s3 ls s3://brandnewbucket

# Get data
aws  --endpoint-url=http://localhost:8000 s3 cp s3://brandnewbucket/shiny_new_object /tmp/retrieved

# Delete data
aws  --endpoint-url=http://localhost:8000 s3 rm s3://brandnewbucket/shiny_new_object
```

## Useful script & tools
You can find several utilities located under ./scripts/.

### Generating keys:

```shell
# Generate PUT key with RS2+1
node scripts/keygen.js scripts/example_hdclient_proxy.conf.json "RS,2,1" genobj 123456 | jq
{
  "parts": {
    "objectKey": "genobj",
    "rand": 123456,
    "code": "RS",
    "nDataParts": 2,
    "nCodingParts": 1,
    "splitSize": 0,
    "data": [
      {
        "location": "localhost:7777",
        "type": "d",
        "fragmentId": 0,
        "hostname": "localhost",
        "port": 7777,
        "key": "genobj-123456-0-d-0"
      },
      {
        "location": "localhost:7777",
        "type": "d",
        "fragmentId": 1,
        "hostname": "localhost",
        "port": 7777,
        "key": "genobj-123456-0-d-1"
      }
    ],
    "coding": [
      {
        "location": "localhost:7777",
        "type": "c",
        "fragmentId": 2,
        "hostname": "localhost",
        "port": 7777,
        "key": "genobj-123456-0-c-2"
      }
    ]
  },
  "genkey": "1#1#1,0#RS,2,1#genobj#123456#localhost:7777#localhost:7777#localhost:7777"
}

```

### Parsing generated keys

```shell
node scripts/keyparse.js 1#1#1,0#RS,2,1#genobj#123456#localhost:7777#localhost:7777#localhost:7777 | jq
{
  "objectKey": "genobj",
  "rand": "123456",
  "splitSize": 0,
  "code": "RS",
  "nDataParts": 2,
  "nCodingParts": 1,
  "data": [
    {
      "location": "localhost:7777",
      "type": "d",
      "fragmentId": 0,
      "hostname": "localhost",
      "port": 7777,
      "key": "genobj-123456-0-d-0"
    },
    {
      "location": "localhost:7777",
      "type": "d",
      "fragmentId": 1,
      "hostname": "localhost",
      "port": 7777,
      "key": "genobj-123456-0-d-1"
    }
  ],
  "coding": [
    {
      "location": "localhost:7777",
      "type": "c",
      "fragmentId": 2,
      "hostname": "localhost",
      "port": 7777,
      "key": "genobj-123456-0-c-2"
    }
  ]
}
```

Invariant of those tools: gen <- keygen(...) && gen["parts] == keyparse(gen["genkey"])
