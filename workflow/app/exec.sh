#!/bin/bash
umask 002
cd /app/workflows
PYTHONPATH='.' luigi --module process_s2_swath "$@"
