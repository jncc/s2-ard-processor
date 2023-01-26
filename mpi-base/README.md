## Building a docker image locally

Deploy local registry:

    docker run -d -p 5000:5000 --restart=always --name registry registry:2

Build docker container

    cd mpi-base
    docker build -t arcsi-mpi-base-dev .

Check ARCSI is working

    docker run arcsi-mpi-base-dev arcsi.py --version

Build an apptainer image using your Docker image

    sudo apptainer build arcsi-mpi-base-dev.sif docker-daemon://arcsi-mpi-base-dev:latest

## Building mpi4py from source
This will need to be done if you change MPI implementations or python versions. It's currently built on JASMIN's OpenMPI 4.0.0 with python 3.10. See https://mpi4py.readthedocs.io/en/stable/install.html#using-distutils.

Steps for the last time it was done (you'll need to be on a JASMIN sci server in a conda env with the same python version as used in the workflow):

    wget https://bitbucket.org/mpi4py/mpi4py/downloads/mpi4py-3.1.4.tar.gz
    tar -zxf mpi4py-3.1.4.tar.gz
    cd mpi4py-3.1.4
    python setup.py build --mpicc=/apps/sw/eb/software/OpenMPI/4.0.0-GCC-8.2.0-2.31.1/bin/mpicc
    python setup.py install --user

If it doesn't build then you may need to follow the hack described in [this issue](https://bitbucket.org/mpi4py/mpi4py/issues/143/build-failure-with-python-installed-from) (copy the `sysconfigdata-conda-user.py` file under the `conf/` directory) and then build like so:

    _PYTHON_SYSCONFIGDATA_NAME=sysconfigdata-conda-user python setup.py build --mpicc=/apps/sw/eb/software/OpenMPI/4.0.0-GCC-8.2.0-2.31.1/bin/mpicc

It should install to `$HOME/.local/lib/python3.10/site-packages` where you can take it and build it into the container.

2023-01-10 update: with python 3.10, the `sysconfig._get_sysconfigdata_name()` function in `sysconfigdata-conda-user.py` no longer allows any arguments so I removed the `True` argument that was being passed in. I can't figure out whether this actually affects anything though, so fingers crossed?!  