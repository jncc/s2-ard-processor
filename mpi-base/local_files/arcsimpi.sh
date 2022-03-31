#!/bin/bash
# Additional bits for Singularity 3 container compatibility
umask 002
PYTHONPATH=/app/local/site-packages
arcsimpi.py $@
