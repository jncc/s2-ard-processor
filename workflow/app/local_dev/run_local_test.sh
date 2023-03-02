cd /<code_path>/s2-ard-processor/workflow/app/workflows
source .venv/bin/activate
PYTHONPATH='.' LUIGI_CONFIG_PATH='/<test_path>/test-luigi.cfg' luigi --module process_s2_swath FinaliseOutputs --testProcessing --local-scheduler
