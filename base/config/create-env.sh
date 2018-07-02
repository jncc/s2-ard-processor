#!/bin/bash
source /etc/profile.d/conda.sh
conda create -n arcsienv python=3.5 -y
conda activate arcsienv
conda install -c conda-forge arcsi -y
conda update -c conda-forge --all -y

