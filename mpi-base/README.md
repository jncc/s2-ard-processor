Deploy local registry:

    docker run -d -p 5000:5000 --restart=always --name registry registry:2

Build docker container

    docker build -t localhost:5000/arcsi-base-mpi .

Push to local registry

    docker push localhost:5000/arcsi-base-mpi 

Build singularity image

    sudo SINGULARITY_NOHTTPS=1 singularity build arcsi-base-mpi.simg docker://localhost:5000/arcsi-base-mpi


