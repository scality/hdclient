Design and Architecture choices
===============================

The hyperdrive client is a new CloudServer_ data backend, responsible
for storing objects inside on-premise hyperdrive servers through one
or more controller. Erase coding capabilities and split are
done by the controller.


All fragments are **IMMUTABLE**. Any mutation is handled at the
CloudServer level.

Glossary
---------

**hyperdrive**
    Scality's own KVS, providing local erasure coding protection over
    multiple disks
**object/objectkey**
    S3 object name to store/retrieve/delete.
**chunk**
    big objects are split into several chunks. Small objects are made
    of only a single chunk.

.. _CloudServer : https://github.com/scality/cloudserver
