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
<genkey> := <version>#<topology_version>#<split>#<rep_policy>#<object_key>#<rand>#<location>[#<location>]
<version> := Natural (so 0 or 1 to start)
<topology_version> := Natural (so 0 or 1 to start) - if we use indirection instead of storing raw UUIDS we must version the mapping
<split> := <n_chunk>,<split_size>
<n_chunk> := number of splitted parts (1 for non-splitted objects)
<split_size> := size of each splitted parts, except last one (see hyperdrive keys below)
<rep_policy> := RS,k,m or CP,n (for n-1 copies)
<object_key> := parent S3 object key (or a prefix) - can contain anything but ‘#’
<rand> := 64 bits random number (unicity inside 1 hyperdrive)
<location>:= hyperdrive location (UUID, idx in table?, ip:port, ...)
```

The keys actually used to sotre fragments on the hyperdrives can be derived easily with only the generated key, even for split.
```
<stored_fragment_key> := <object_key>-<rand>-<start_offset>-<type>-<fragid>
<object_key> and <rand> are the ones defined above
<type> := ‘d’ for data, ‘c’ for coding
<fragid>:= index in main key fragment list
<start_offset> := used for splits. All split chunks share the same prefix, storing the offset is used to easily have range queries and avoid storing them all in the main key.
```

Meets all requirements (except perhaps around the last stripe size of an ECN chunk). Split is required to contact the same hyperdrives for the object: all chunks of an object generates the same number of fragments, and fragments X of chunks Y is always stored onto selected hyperdrive X. This has several benefits: only selecting once, object located on only a handful of hyperdrives => less machines to contact = less opportunities to fail or hit a straggler. The way split is handled also enables us not to have a manifest, worries about its size, location or freshness.

Note: key generation requires setting aside a single character and forbid the users to it in their object name. We currently settled on '#'. Handling of the maximum key length is still vague (some pending work on the hyperdrive and probably prefixing on hdclient side).

Example key:
1/ Small key: storing s3://fakebucket/obj1 with RS2+1 onto hd1, hd2 and hd3
Main key: 1#1#1,0#RS,2,1#obj1#314159#hd1#hd3#hd2
Hyperdrive keys:
1. obj1-314159-0-d-1
2. obj1-314159-0-d-2
3. obj1-314159-0-c-3

2/ Splitted key: storing s3://fakebucket/Large1 with RS2+1 onto hd1, hd2 and hd3
Key size: 64000, split_size: 49000, 2 parts on same hyperdrive (no conflict!)

Main key: 1#1#2,49000#RS,2,1#Large1#42#hd3#hd2#hd3
Hyperdrive keys:
* On hd1: none
* On hd2
  1. Large1-42-0-d-2
  2. Large1-42-49000-d-2
* On hd3
  1. Large1-42-0-d-1
  2. Large1-42-0-c-3
  3. Large1-42-49000-d-1
  4. Large1-42-49000-c-3

## Error handling

### On DELETE

Failure to delete a fragment ( != 20X, 404) should not fail the overall operation but must log an appropriate entry into designated Kafka topic, to be cleaned later on by HdRepair.

Topic: delete
Content: JSON messages
Example JSON:
```json
{
    "rawKey": .... # associated hdclient metadata
    "toDelete": [ #[chunkId, fragmentId]
        [1, 1],
        [1, 2],
        [2, 1],
        ...
    ]
}
```

### On PUT

Failure ot PUT any fragment (erasure coded, replicated or split) should fail the overall operation. All fragments already PUT pending must be cleaned afterwards. Cleaning is handled in the same way as orphans created on deletion (refer to On DELETE section).

Special case of timeout/connection close: a timeout on a PUTing a fragment leaves no clear signal. Is it stored? Queued but not yet written? We could either:
1. Consider it as not OK and fail the overall PUT
2. Consider it OK BUT we must log a Kafka entry requesting to check its real status asap (perhaps logging it as a repair action)

**Note: we may accept such unsure fragment trick IFF we have enough fragments already stored.** For replication, we must be sure of at least 1 (or 2?). For Reed-Solomon k+m we need at least k real OK (or k+1?).

### On GET

If we are able to provide the caller with the data, we must do so. For replication it equals being able to contact and read data from a single hyperdrive. For erasure coding we must repair data online if we can. In any case, a detected error (404, correupted or else) must be logged and a corresponding Kafka entry must be persisted, asking hdrepair to check and do its magic if need be. Only exception is failure to contact a hyperdrive.

## Data placement

TODO
