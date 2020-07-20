#!/bin/bash
umask 002
PYTHONPATH=/app/local/site-packages
arcsimpi.py $@
