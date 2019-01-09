    #!/bin/bash
    echo $SHELL
    # mute debconf warnings
    DEBIAN_FRONTEND=noninteractive

    #Install packages
    apt-get update && apt-get install apt-utils -y 

    # add debian packages required by arcsi
    apt-get update && \
    apt-get install -y \
        libmpfr4 \
        libgl1-mesa-glx && \
    apt-get clean

    # update conda and install arcsi using conda package manager and clean up (rm tar packages to save space)
    conda update -n base conda
    conda update --yes -c conda-forge conda && \
    conda install --yes -c conda-forge python=3.5 arcsi && \
    conda update -c conda-forge --all && \
    conda clean --yes -t

    # Create processing paths
    mkdir -p /data
