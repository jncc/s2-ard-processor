S1 ARD Processor
================

Docker container that runs the s1-processing-scripts.

S1 processing scripts subtree
--------------------
The S1 processing scripts branch is required at build time

### Add the remote
Create a link to the proper remote repo

    git remote add -f s1-processing-scripts https://github.com/jncc/s1-processing-scripts.git


### Linking to the correct branch
At development time you will need to link to a development branch for testing.
Before building a production contain this branch should be merged into master and this project should be set to master.

To change s1-processing-scripts to a different branch:

    git rm -rf workflow/app/toolchain/scripts
    git commit
    git subtree add --prefix=workflow/app/toolchain/scripts s1-processing-scripts <branch> --squash

Current:

    git subtree add --prefix=workflow/app/toolchain/scripts s1-processing-scripts master --squash

### Fetching changes from the subtree

All changes to the current working tree need to be commited

    git fetch s1-processing-scripts
    git subtree add --prefix=workflow/app/toolchain/scripts s1-processing-scripts <branch> --squash

### Pulling changes from the subtree

    git subtree pull --prefix=workflow/app/toolchain/scripts s1-processing-scripts <branch> --squash

Current:

    git subtree pull --prefix=workflow/app/toolchain/scripts s1-processing-scripts master --squash

### Pushing changes from the subtree back to the repo

    git subtree push --prefix=workflow/app/toolchain/scripts s1-processing-scripts <branch> --squash

Current:

    git subtree push --prefix=workflow/app/toolchain/scripts s1-processing-scripts master --squash

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
    -t jncc/test-s1-ard-processor 

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
        
        s2-ard-processor.simg /app/exec.sh 
            FinaliseOutputs 
            --dem=dem.kea 
            --outWkt=outwkt.txt 
            --projAbbv=osgb
            --metadataConfigFile=metadata.config.json 
            --metadataTemplate=metadataTemplate.xml 
            --local-scheduler

## Runtime parameters
--metadataTemplate - can be overridden with a template path relative to the container - ie /working/templates/mytemplate.xml
--metadataConfigFile=metadata.config.json 
--metadataTemplate=metadataTemplate.xml 
--dem=The digital elevation model 

### Optional parameters
--outWkt - the wkt file supplied to arcsi
--projAbbv - The target projection abriviation

