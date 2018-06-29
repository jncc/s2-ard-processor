#!/bin/bash
source /etc/profile.d/conda.sh
conda create -n arcsi361 python=3.5 -y
conda activate arcsi361
conda install -c conda-forge arcsi -y
conda update -c conda-forge --all -y

