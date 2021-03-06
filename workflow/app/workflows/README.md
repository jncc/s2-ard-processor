# eo-s2-workflow
Sentinel 2 ARD workflow

# Running

To run the workflow, you need to have and be running in an existing Arcsi base 
container (or other environment with Arcsi installed), this process will be 
wrapped up in a docker container, and will require the following inputs;

PYTHONPATH='.' luigi --module process_s2_swath FinaliseOutputs --outWkt osgb.wkt --projAbbv osgb --dem dem.kea --local-scheduler

To test the workflow on your local dev machine without needing a container, you can also use the --testProcessing flag. This will skip most of the ARCSI/heavy processing commands, but you'll need to set up the venv yourself:

Create virtual env
```
virtualenv -p python3 /<project path>/eo-s2-workflow-venv
```
Activate the virtual env
```
source ./eo-s2-workflow-venv/bin/activate
```
Install Requirements
```
pip install -r requirements.txt
```

## PathRoots and input folders

The `luigi.cfg` file contains the default parameters for the processing chain, currently it only contains the `pathRoots` json object that defines the following variables;

```json
{
    "input": "/workflow/input",
    "static": "/workflow/static",
    "state": "/workflow/state",
    "output": "/workflow/output"
}
```

### input

`/workflow/input`

A folder containing the input granule files as zip files directly downloaded from a Copernicus data source such as SciHub

### static

`/workflow/static`

A folder containing the static input files for this processing chain, two files will exist here;

 - DEM file - a digital elevation model converted and stored as a KEA file in the required output projection format
 - Projection WKT - a WKT representation of the request output projection as OGC WKT

### state

`/workflow/state`

A folder that will contain the state files for this job, this can be output at the end of the process 

### ouptut

`/workflow/output`

A folder that will contain the requested output files, converted to tif with thumbnails, metdata, etc...

## Task Dependencies

Each section (seperated by ------------) is a functional step or (set of steps that can be run concurrently) that depends on the task in the above section giving a sort of dependency graph

| Task                           | Spawns (one or more*)           |
|--------------------------------|---------------------------------|
| UnzipRaw                       |                                 |
|--------------------------------|---------------------------------|
| GetInputFileInfos              | GetInfputFileInfo*              |
| GetSatelliteAndOrbitNumber     |                                 |
|--------------------------------|---------------------------------|
| BuildFileList                  |                                 |
|--------------------------------|---------------------------------|
| ProcessRawToArd                | CheckFileExistsWithPattern*     |
|--------------------------------|---------------------------------|
| ConvertToTif                   | GdalTranslateKeaToTif*          |
|--------------------------------|---------------------------------|
| OptimiseOutputs                | BuildPyramidsAndCalculateStats* |
| GenerateMetadata               | GenerateProductMetadata*        |
|--------------------------------|---------------------------------|
| GenerateThumbnails             | GenerateThumbnail*?             |
|--------------------------------|---------------------------------|
| FinaliseOutputs                |                                 |

## Calling the workflow (job-specs) - subject to change

Given a 'complete' swath we should be able to start up multiple jobs at the same time with little effort, so assuming a complete swath we can generate a job-spec to run this in a production environment

```json
{
    "product": "name-of-product [can just be a process time name or id for the job]",
    "processor": "name-of-processor [i.e. jncc/s2-ard-process@0.0.1]",
    "input-path": [
        "a list of paths for each of the raw data files that are needed for this process to run",
        "may be already local to a machine, but the instigator of the job should move these files",
        "so that they are accessible as a folder mount to a docker container",
        "x",
        "y",
        "z"
    ],
    "attempted": "0 # Count of the number of attempts have been made to process this product"
}
```
