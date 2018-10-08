# Design and Architecture choices

The hyperdrive client is a new CloudServer data backend, repsonsible for storing objects inside on-premises hyperdrive servers. It must support several erasure coding capabilites, splitting large objects, select the best available hyperdrives to store data on.

As a data backend it must construct its own metadata in form of a key/string, returned to CloudServer on PUT. This metadata is stored by CloudServer inside its own metadata backend and is provided to the client on GET or DELETE.

All fragments are IMMUTABLE. Any mutation is handled at the CloudServer level.

## Glossary

* **object/objectkey**: S3 object name to store/retrieve/delete.
* **chunk**: big objects are splitted into several chunks. Small objects are made of only a single chunk.
* **fragment**: part of a chunk. For replication, it is a full copy. For erasure coding, it is a data or coding part.
* **CP,n**: replication/copy where n is the final number of fragments written. CP,2 means the original and 1 copy.
* **RS,k,m**: systematic Reed-Solomon erasure code with k data segments and m parities.
* **rawKey/genKey**: metadata created by hdclient to track all fragments of an object, encoded as a string and returned on PUT to CloudServer for persistent storage.

## Key generation scheme

List of detailed requirements:

|Requirement | Description |
| ---------- | ------------|
| Version | A perfect solution often does not exists. And will not stay that way. Generated keys MUST start with a version, so that we can easily change the format. |
| Relate easily to parent CloudServer objectkey | Would make life easy to be able to relate a fragment key with its parent, main object. |
| Small and self describing | Smallest keys possible, obviously. But without relying too much on implicit information. Generated keys should be as self describing as possible. |
| Embed replication policy | Describe what code is used. Typically RS2+1 or RSk+m or even replication factor for very small object (if we decide to go that way). We could then add new code types easily. |
| Split ready | A splitted key should be obvious to spot. The generated key length should NOT depend on number of chunk. |
| Fragment name | Resulting fragment key stored on hyperdrive must contain the parent, S3 object name. Or at least a prefix if too long. Resulting key must also be unique hyperdrive-wise. |
| Fragment location | Each fragment must enable the hdclient to select the correct hyperdrive. |
| Fragment type | Each fragment should be tagged as data or coding (reading would then first prefer data parts since RS is systematic). |

Since we can specify which key to use on an hyperdrive, we could define a new key scheme to encode fragments in a unified key scheme. The idea is to minimize the amount of metadata we have to store with every object in the metadata database that can be used to locate the fragments. Note that we plan to use a 2+1 or 4+2 erasure code for now so the overhead of storing the full uuids for fragment location is not yet important. When the time comes, we are currently considering having an indirection layer mapping short ids (best would be 1,2,3 etc) to hyperdrive UUIDS. This mapping could be versioned, referred ad topology_version below. All fragment IDs should start with the same key as the object (part) key. If the object (part) key is indexed we can quickly find to which object belong the missing fragment (e.g. for repair, see below). The hdclient backend in cloud server will return this constructed key to cloud server main code.

Proposed key generation scheme:
```
<genkey> := <version>#<serviceId>#<split>#<rep_policy>#<ctime>#<hash>#<location>[#<location>]
<version> := Natural (so 0 or 1 to start)
<serviceId> := Natural > 1 - serviceId, can be used for namespacing
<split> := <size>,<split_size>
<size> := total size of the object
<split_size> := size of each splitted parts, except last one (see hyperdrive keys below)
<rep_policy> := RS,k,m,stripe_size or CP,n (for n-1 copies) - stripe_size is a positive integer
<ctime> := creation timestamp of the key
<hash> := 64 bit hash of bucketName/objectKey/version, in hexadecimal
<location>:= hyperdrive location (UUID, idx in table?, ip:port, ...)
```

The keys actually used to sotre fragments on the hyperdrives can be derived easily with only the generated key, even for splits.
```
<stored_fragment_key> := <serviceId>-<ctime>-<hash>-<end_offset>-<fragid>
<serviceId>, <ctime> and <hash> are the ones defined above
<fragid>:= index in main key fragment list
<end_offset> := used for splits, exclusive. All split chunks share the same prefix, storing the offset is used to easily have range queries and avoid storing them all in the main key. End of chunk offset is used to be able to infer object real size from last chunk keys alone (only way to do it for erasure coded last chunk parts).
```

Meets all requirements (except perhaps around the last stripe size of an ECN chunk). Split is required to contact the same hyperdrives for the object: all chunks of an object generates the same number of fragments, and fragments X of chunks Y is always stored onto selected hyperdrive X. This has several benefits: only selecting once, object located on only a handful of hyperdrives => less machines to contact = less opportunities to fail or hit a straggler. The way split is handled also enables us not to have a manifest, worries about its size, location or freshness.

Note: key generation requires setting aside a single character and forbid the users to it in their object name. We currently settled on '#'. Handling of the maximum key length is still vague (some pending work on the hyperdrive and probably prefixing on hdclient side).

Example key:
1/ Small key: storing s3://fakebucket/obj1/11 of 32KB with RS2+1 (stripe: 4096) onto hd1, hd2 and hd3
Main key: 1#42#32768,32768#RS,2,1,4096#123456789#deadbeef#hd1#hd3#hd2
Hyperdrive keys:
1. 42-123456789-deadbeef-320000-0
2. 42-123456789-deadbeef-320000-1
3. 42-123456789-deadbeef-320000-2

2/ Splitted key: storing s3://fakebucket/Large1/13 with RS2+1,4096 onto hd1, hd2 and hd3
Key size: 64000, split_size: 49000, 2 parts on same hyperdrive (no conflict!)

Main key: 1#42#64000,49000#RS,2,1,4096#123456456#cafebabe#hd3#hd2#hd3
Hyperdrive keys:
* On hd1: none
* On hd2
  1. 42-123456456-cafebabe-49000-2
  2. 42-123456456-cafebabe-64000-2
* On hd3
  1. 42-123456456-cafebabe-49000-1
  2. 42-123456456-cafebabe-49000-3
  3. 42-123456456-cafebabe-64000-1
  4. 42-123456456-cafebabe-64000-3

## Error handling

### On DELETE

Failure to delete a fragment should not fail the overall operation but must log an appropriate entry into designated Kafka topic, to be cleaned later on by HdRepair.

A successful fragment query is considered successful iff resulted in 20X or 404. Everything else, included timeouts, are considered as failures. If any fragment is in error, it must be recorded and persisted in 'delete' topic (Kafka I guess). Failure to persist should fail the overall DELETE. If any fragment succeeds and any errors has been persisted, then DELETE succeeds.

Example of 'delete' topic entry: expects JSON messages
```json
{
    "rawKey": .... # associated hdclient metadata
    "fragments": [ #[chunkId, fragmentId]
        [1, 1],
        [1, 2],
        [2, 1],
        ...
    ]
}
```

### On PUT

A fragment is considered successfully stored iff no error was returned or an timeout. Object PUT is considered successful iff ALL fragments of ALL chunks are considered OK, with only exception of 'too many' fragments in timeout. This threshold is per chunk, with 'too many' = ~50% for replication, nCoding for erasure coding.

Upon operation failure, all successful fragments must be cleaned up. Cleaning is handled in the same way as orphans created on deletion (refer to On DELETE section).

Fragments in limbo - ie whose status is unser as on timeout - must also be persisted in a given topic 'check', instructing other processes to quickly ascertain its status. This 'check' topic has the same layout as of 'delete' topic:

Example of 'delete' topic entry: expects JSON messages
```json
{
    "rawKey": .... # associated hdclient metadata
    "fragments": [ #[chunkId, fragmentId]
        [1, 1],
        [1, 2],
        [2, 1],
        ...
    ]
}
```

### On GET

If we are able to provide the caller with the data, we must do so. For replication it equals being able to contact and read data from a single hyperdrive. For erasure coding we must repair data online if we can. In any case, a detected error (404, correupted or else) must be logged and a corresponding Kafka entry must be persisted, asking hdrepair to check and do its magic if need be. Only exception is failure to contact a hyperdrive. Repair topic has exactly the same layout as 'delete' and 'check' topics.

Example of 'repair' topic entry: expects JSON messages
```json
{
    "rawKey": .... # associated hdclient metadata
    "fragments": [ #[chunkId, fragmentId]
        [1, 1],
        [1, 2],
        [2, 1],
        ...
    ]
}
```

## Data placement

TODO
