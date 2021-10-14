GDAL version is determined by arcsi dependancy graph

build
=====

* Bump library versions for metadata inc GDAL when we know what it is.

mpi-base
========

Still needed to encapsulate JASMIN specific bindings.

* Build from (latest?) petebunting/au-eoed(-dev) container? - EO/PB to establish version, otherwise build from arcsi-base
* sysconfigdata-conda-user.py -- is this hack still needed?
* arcsimpi.py -- any updates?
* upgrade openmpi version



workflow
========

* /app/workflows/requirements.txt -- probable version updates

EO team tasks
=============

* Deterimine appropriate versions of arcsi and gdal to use - This will probably require input from devs to build vms for testing.
* EO need to verify that the ard works with the versions indicated - manually process a scene.
* If we are using pete buntings container then it would ideal if we could test the process manually with that. 
* We will need to provide a ubuntu 20.04 vm for them (somewhere?)
* QA / QC outputs from new containers. 
    * western edge effect
    * flipped lat / lng.
