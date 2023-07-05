S2 ARD Processor
================

Docker container that runs the s2-ard-processor workflow.

The mapped input folder contains a set of S2 granules that will be processed as a swath. The processing can take place sequentially or in parallel using MPI on the JASMIN cluster.

Build and run instructions
--------------------------

Build to image:

    cd workflow
    docker build -t s2-ard-processor .

Use `--no-cache` to build from scratch

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

Where \<hostpath> is the path on the host to the mounted folder

Convert Docker image to apptainer image
---------------------------------------

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

Code change and deployment process
----------------------------------

The code in this repo will be jointly maintained by JNCC and DEFRA/CGI. Use the steps below as a guideline for making new changes:

1. Create a new `feature` branch from `main` and commit your changes there until you're ready to merge
2. Open a pull request to merge back into `main` and add a reviewer from both JNCC and CGI to notify them
3. JNCC then uses the `feature` branch to build a [jncc/s2-ard-processor-dev](https://hub.docker.com/r/jncc/s2-ard-processor-dev) docker image which both parties can use for testing and QA
4. Once it passes QA, JNCC will approve the PR, merge it into `main`, and build a live [jncc/s2-ard-processor](https://hub.docker.com/r/jncc/s2-ard-processor) docker image which can be deployed to production