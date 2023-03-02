S2 ARD Processor
================

Docker container that runs the s2-ard-processor workflow.

The mapped input folder contains a set of S2 granules that will be processed as a swath. The processing can take place sequentially or in parallel using MPI on the JASMIN cluster.

Build and run instructions
--------------------------

Build to image:

    cd workflow
    docker build -t s2-ard-processor .

Use --no-cache to build from scratch

Run Interactively:

docker run -i --entrypoint /bin/bash 
    -v /<hostPath>/input:/input 
    -v /<hostPath>/output:/output 
    -v /<hostPath>/state:/state 
    -v /<hostPath>/static:/static 
    -v /<hostPath>/working:/working 
    -v /<hostPath>/report:/report
    -v /<hostPath>/database:/database 
    -t s2-ard-processor

Where <hostpath> is the path on the host to the mounted folder

Convert Docker image to apptainer image
-----------------------------------------

Build an apptainer image using your Docker image

    sudo apptainer build s2-ard-processor.sif docker-daemon://s2-ard-processor:latest

Run:

    apptainer exec 
        --bind /<hostPath>/input:/input 
        --bind /<hostPath>/output:/output 
        --bind /<hostPath>/state:/state 
        --bind /<hostPath>/static:/static 
        --bind /<hostPath>/working:/working
        --bind /<hostPath>/report:/report
        --bind /<hostPath>/database:/database
        
        s2-ard-processor.sif /app/exec.sh 
            GenerateReport
            --dem=dem.kea 
            --outWkt=outwkt.txt 
            --projAbbv=osgb
            --metadataConfigFile=metadata.config.json 
            --metadataTemplate=metadataTemplate.xml
            --reportFileName=reportfile.csv
            --dbFileName=s2ardProducts.db
            --local-scheduler


For the full list of parameters and more details on the folder setups, see the workflow readme at `workflow/app/workflows/README.md`.