======================
Implementation details
======================

Code is located under ../src/ folder. HdClient must respect the
`CloudServer data backend API`_,
and hence must work exclusively with nodejs streams.

Main entry point and helper files
---------------------------------

The definition of the data backend is in hdcontroller.js. The file
implements everything needed to store/read/delete the data.

Most files are self explanatory and already in-depth explanation or
where to find them:

* shuffle.js: a small helper to randomize the bootstrap list.

.. _`CloudServer data backend API` : https://github.com/scality/cloudserver/tree/development/8.1/docs/developers
.. _Design : ../Design.rst
