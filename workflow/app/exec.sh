#!/bin/bash
cd /app/workflows
PYTHONPATH='.' luigi --module process_s2_swath "$@"
