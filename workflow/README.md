S2 ARD Processor
================

Docker container that runs the s2-processing-scripts.

The mapped input folder contains a set of S2 granules that will be processed as a swath. The processing can take place sequentially or in parallel using MPI on the JASMIN cluster.

Build and run instructions
--------------------------

Build to image:

    docker build -t s1-ard-processor .

Use --no-cache to build from scratch

Run Interactivly:

docker run -i --entrypoint /bin/bash 
    -v /<hostPath>/input:/input 
    -v /<hostPath>/output:/output 
    -v /<hostPath>/state:/state 
    -v /<hostPath>/static:/static 
    -v /<hostPath>/working:/working 
    -v /<hostPath>/report:/report 
    -t jncc/test-s2-ard-processor 

Where <hostpath> is the path on the host to the mounted fole

Convert Docker image to Singularity image
-----------------------------------------

Create a Singularity file

    Bootstrap: docker
    Registry: http://localhost:5000
    Namespace:
    From: s2-ard-processor:latest

Run a local docker registry
	
    docker run -d -p 5000:5000 --restart=always --name registry registry:2

Tag and push your docker image to the registry

    docker tag s2-ard-processor localhost:5000/s2-ard-processor
    docker push localhost:5000/s2-ard-processor

Build a Singularity image using your Docker image

    sudo SINGULARITY_NOHTTPS=1 singularity build s2-ard-processor.simg Singularity

Run:

    singularity exec 
        --bind /<hostPath>/input:/input 
        --bind /<hostPath>/output:/output 
        --bind /<hostPath>/state:/state 
        --bind /<hostPath>/static:/static 
        --bind /<hostPath>/working:/working
        --bind /<hostPath>/report:/report
        
        s2-ard-processor.simg /app/exec.sh 
            GenerateReport
            --dem=dem.kea 
            --outWkt=outwkt.txt 
            --projAbbv=osgb
            --metadataConfigFile=metadata.config.json 
            --metadataTemplate=metadataTemplate.xml
            --reportFileName=reportfile.csv
            --dbFileName=s2ardProducts.db
            --local-scheduler

## Runtime parameters
--metadataTemplate - can be overridden with a template path relative to the container - ie /working/templates/mytemplate.xml
--metadataConfigFile=metadata.config.json 
--metadataTemplate=metadataTemplate.xml 
--dem=The digital elevation model 
--testProcessing=Only run through the workflow logic creating dummy files where needed. Do not process.
--reportFileName=A csv file that will be created in the report folder. This file contains an entry for each processed granule.
--dbFileName=An sqlite database file to which the report data is also written. The data is written to the s2ArdProducts table.

### Optional parameters
--outWkt - the wkt file supplied to arcsi
--projAbbv - The target projection abriviation

### Jasmin specific parameters
--arcsiCmdTemplate - A template file for the arcsi command


# static folder
- wkt file
- dem
- metadata-config.json file

{
    "projection" : "OSGB",
    "targetSrs" : "EPSG:27700",
    "demTitle" : "dem title",
    "placeName" : "United Kingdom",
    "parentPlaceName" : "Europe"
}

- Arcsi command template.


# If running MPI jobs
- jasmin-mpi-config.json file (in root of static folder)

{
    "container" : {
        "location" : "/img/path/imag.simg",
        "mounts" : [
            ("/host/path","/container/path"),
            ("/host/path2","/container/path2")
        ]
    }
    "jobTemplate" : "s2_mpi_job_template.bsub"
}

- mpi-template.bsub

# Input folder
This contains a set of raw S2 granules that will be processed as a swath either in sequence or simultaneiously using MPI
