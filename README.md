# NodeJS hyperdrive client API

## Contributing

In order to contribute, please follow the
[Contributing Guidelines](
https://github.com/scality/Guidelines/blob/master/CONTRIBUTING.md).

## Overview

The Hyperdrive client is managing the Hyperdrive Controller endpoint for CloudServer.
It exposes a simple API for a group of HDController.

It does not change the data, it is merely a proxy to HDController.

### HDController protocol

Basically a subset of HTTP 1.1 using POST, GET and DELETE verbs to create, to get and to delete an object.

On POST, the HDController will create a key that can be used in GET / DELETE to access the data stored.

## Usage

### Installation

```shell
npm install --save scality/hdclient
# Check for dependencies vulnerabilities
npm audit
```

### Running tests

To use the linter and run the tests:

```shell

# All tests
npm test tests/

# Other options and Mocha help
npm test -- -h

# Code coverage
npm run coverage
```

### Generating documentation

```shell
# Generating JSDoc, to start browsing open docs/jsdoc/index.html
npm run jsdoc
```

### Performance diagnostic tool

[Node-clinic](https://github.com/nearform/node-clinic) is installed by default as dev dependency. It can be used to diagnose general performance, I/O specific, event-loop issues, etc. It can also be used to generate flame graphs. Below are some usage examples usage. Note that data acquisition and visualization can be sepearated, they are not in the examples.

```shell
# Node clinic diagnosis tool - help
npm run clinic

# Clinic doctor help
npm run clinic doctor

# Diagnosing on the same machine - use --on-port to kickstart load generator
NODE_ENV=production npm run clinic doctor -- \
                   --on-port='for i in {..10}; do curl -XPUT --data-binary @/etc/hosts "http://localhost:6767/bucket/testobj$i" ; done' \
                   -- node scripts/server.js 6767 scripts/example_hdclient_proxy.conf.json

NODE_ENV=production npm run clinic doctor -- \
                   -- node scripts/server.js 6767 scripts/example_hdclient_proxy.conf.json &
pid=$!
# Start and wait for load generator to finish from somewhere else...
kill -SIGINT $pid # Or keep process in foreground and Ctrl-C when done
wait $pid

# Flame graph help
npm run clinic flame

# Flame graph - load-generator on different machine, to be started whenever the server is up
NODE_ENV=production npm run clinic flame -- node scripts/server.js <port> <config file>
...
Ctrl-C
Analysing data
...
```

### Running as standalone

Because deploying the full S3 server might be too much of a hassle for your specific need, a HTTP server using HdClient is provided. The mapping between object key and internal keys (the ones actually stored on the hyperdrive) is stored in-memory only. The internal keys are not accessible on the outside, mirrorring behavior of Zenko-like deployment.

 ```shell
# Start Hyperdrive 'proxy'
# example conf assumes 1 hdcontroller listening on localhost:18888
NODE_ENV=production node scripts/server.js 8888 scripts/example_hdclient_proxy.conf.json &

# Have fun
curl -XPUT --data-binary @/etc/hosts -v http://localhost:8888/mybucket/testobj
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

# <url>/<bucket>/<object>/<version>
curl -XDELETE -v http://localhost:8888/mybucket/testobj/64
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

How to run integrated hyperdrive client inside S3 or Zenko deployment? This section is only a work in progress
since actual S3 integration code is not yet merged.

```shell
# Checkout S3 repository and checkout proper hdclient integration branch
git clone https://github.com/scality/CloudServer.git
cd CloudServer/
git checkout feature/RING-28500-add-hyperdrive-client-data-backend-real

# Modify package.json to use the version of hdclient you want in case latest development/1.0 is not good
# To use a local repository
# sed s%scality/hdclient%file:<path to hdclient repository% package.json
# To use a tag or commit
# sed -i s%scality/hdclient%scality/hdclient#<tag/commit>% package.json

# Add new locationConstraints
# Region us-east-1 is mandatory, since the default config still references it
cat <<EOF > hdclient_locationConfig.json
{
    "us-east-1": {
        "type": "file",
        "objectId": "iod1",
        "legacyAwsBehavior": true,
        "details": {}
    },
    "hyperdrive-cluster-1": {
        "type": "scality",
        "objectId": "oid2",
        "legacyAwsBehavior": false,
        "details": {
            "connector": {
                "hdclient" : {
                  "bootstraps": "localhost:18888",
                  "path": "/store/",
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

# Start a Kafka instance on passed kafkaBrokers parameters (in the example 127.0.0.1:6666)

# Start CloudServer (memory backend ie metadata in-memory)
# More informations inside S3 repository documentation
NODE_ENV=production S3DATA=multiple S3_LOCATION_FILE=hdclient_locationConfig.json npm run mem_backend
```

In a separate tab, have fun with AWS CLI

```shell
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
