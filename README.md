# S2 ARD Processor

Docker based sentinel 2 Analysis ready production system, based on https://github.com/mundialis/docker-arcsi

## Base

A base docker image packaging Dr Pete Buntings Python Atmospheric and Radiometric Correction of Satellite Imagery (ARCSI) software (https://www.arcsi.remotesensing.info/).

Based on the official ContinuumIO Miniconda3 release with python 3.5, base package contains a minimal installaition of ARCSI and its dependencies using the conda package manger, correct as of version 3.1.6 (conda reporting 3.6.1).

### Build or Pull arcsi-base

#### Build image

`docker build -t jncc/arcsi-base ./base/`

**OR**

#### Pull Image direction from docker hub

`docker pull jncc/arcsi-base`

### Usage

#### Run image interactively

`docker run -i -v <local mount point>:/data -t jncc/arcsi-base /bin/bash`

To run a container and get help on ARCSI commandline options do:

`docker run -t mundialis/arcsi arcsi.py -h`

See below under "Docker example" for a more detailed Sentinel-2 example.

### Docker example

``` bash
# define name of Sentinel-2 scene - note: omit: .SAFE
S2IMG=S2A_MSIL1C_20170327T105021_N0204_R051_T31UFT_20170327T105021
DEM=srtm_30m_myregion.tif
MY_S2_PATH=/path/to/S2_data/
MY_DEM_PATH=/path/to/DEM_data/

# run ARCSI (we use volume mapping to make S2 and DEM visible inside the docker container)
docker run -i -t -v ${local_data}:/data jncc/arcsi-base \
    arcsi.py -s sen2 --stats -f KEA --fullimgouts -p RAD SHARP SATURATE CLOUDS TOPOSHADOW STDSREF DOSAOTSGL METADATA FOOTPRINT \
    --interp near --outwkt /data/${PATH_TO_OUTPUT_PROJ_WKT} --projabbv ${PROJ_ABBREVIATION} -t /data/tmp/ -o /data/output/ \
    --dem /data/${PATH_TO_DEM} -i /data/inputs/${SINGLE_INPUT_FILE}
```

### See also

Thanks to Markus Neteler, Edward P. Morris and Angelos Tzotsos for their work on the orignal ARCSI Dockerfile.