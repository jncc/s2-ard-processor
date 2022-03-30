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
This will need to be done if you change MPI implementations or python versions. It's currently built on JASMIN's OpenMPI 4.1.1 with python 3.5. See https://mpi4py.readthedocs.io/en/stable/install.html#using-distutils.

Steps for the last time it was done (you'll need to be on a JASMIN sci server in a conda env with the same python version as used in the workflow):

    wget https://bitbucket.org/mpi4py/mpi4py/downloads/mpi4py-3.1.3.tar.gz
    tar -zxf mpi4py-3.1.3.tar.gz
    cd mpi4py-3.1.3
    python setup.py build --mpicc=/apps/eb/software/OpenMPI/4.1.1-GCC-10.3.0/bin/mpicc
    python setup.py install --user

If it doesn't build then you may need to follow the hack described in [this issue](https://bitbucket.org/mpi4py/mpi4py/issues/143/build-failure-with-python-installed-from) (copy the `sysconfigdata-conda-user.py` file under the `conf/` directory) and then build like so:

    _PYTHON_SYSCONFIGDATA_NAME=sysconfigdata-conda-user python setup.py build --mpicc=/apps/eb/software/OpenMPI/4.1.1-GCC-10.3.0/bin/mpicc

It should install to `$HOME/.local/lib/python3.5/site-packages` where you can take it and build it into the container.