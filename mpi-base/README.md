## Building a singularity container locally
You'll need docker and singularity installed and at least 5GB free space.

Deploy local registry:

    docker run -d -p 5000:5000 --restart=always --name registry registry:2

Build docker container

    docker build -t localhost:5000/arcsi-base-mpi .

Push to local registry

    docker push localhost:5000/arcsi-base-mpi 

Build singularity image

    sudo SINGULARITY_NOHTTPS=1 singularity build arcsi-base-mpi.simg docker://localhost:5000/arcsi-base-mpi


## Building mpi4py from source
This will need to be done if you change MPI implementations or python versions. It's currently built on JASMIN's OpenMPI 4.0.0 with python 3.5. See https://mpi4py.readthedocs.io/en/stable/install.html#using-distutils.

Steps for the last time it was done:

    wget https://bitbucket.org/mpi4py/mpi4py/downloads/mpi4py-3.0.3.tar.gz
    tar -zxf mpi4py-3.0.3.tar.gz
    cd mpi4py-3.0.3

Follow the hack described in [this issue](https://bitbucket.org/mpi4py/mpi4py/issues/143/build-failure-with-python-installed-from) to get it to build successfully in the conda environment, then build it:

    conda activate python3env
    _PYTHON_SYSCONFIGDATA_NAME=sysconfigdata-conda-user python setup.py build --mpicc=/apps/eb/software/OpenMPI/4.0.0-GCC-8.2.0-2.31.1/bin/mpicc
    python setup.py install --user

It should install to `$HOME/.local/lib/python3.5/site-packages` where you can take it and build it into the container.