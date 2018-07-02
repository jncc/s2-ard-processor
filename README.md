# S2 ARD Processor

Docker based sentinel 2 Analysis ready production system

## Base
A base docker image with arcsi installed

### Build and run instaructions

#### Build image:

    docker build -t s2-ard-processor-base .

### Push images to JNCC docker hub repo

```
docker login --username=<dockerhub username>
docker tag s2-ard-processor-base jncc/s2-ard-processor-base:<Arcsi version>
docker push jncc/s2-ard-processor-base
```

#### Run image interactivly

    docker run -i -v <local mount point>:/data -t s2-ard-processor-base /bin/bash

#### Run Arcsi ineractivly:

From docker console:

```
source /etc/profile.d/conda.sh
conda activate arcsi361
arcsi.py --version
```


