# Sentinel 2 ARD workflow

## Prerequisites

* An ARCSI v4 installation/container
* Python 3.6+ (we use 3.10)
* A Digital Elevation Model in kea format, and output projection in wkt format (as required by ARCSI)
* A working area with folder setup like so:
  * `/input` containing your inputs, e.g. S2B_MSIL1C_20220620T110629_N0400_R137_T30UXD_20220620T115229
  * `/output`
  * `/working`
  * `/static` containing your DEM, WKT, and metadata config file
  * `/state`

## Setup your virtual env

Create virtual env

    python3 -m venv .venv

Activate the virtual env

    source .venv/bin/activate

Install Requirements

    pip install -r requirements.txt


## Create a luigi.cfg

You can create your own config file by using `luigi.cfg.template` as a guide. Replace the filepaths as necessary to match your local setup. Note that the `dem`, `outWkt`, and `metadata-config.json` files are expected to found be in the `static` directory.

## Run the workflow

    PYTHONPATH='.' LUIGI_CONFIG_PATH='luigi.cfg' luigi --module process_s2_swath FinaliseOutputs --local-scheduler

Where PYTHONPATH is the path to the process_s2_swath directory and LUIGI_CONFIG_PATH is the path to your luigi.cfg file. 

## Local dev

You can test the workflow logic without actually needing to install ARCSI by using the `--testProcessing` flag. This will skip most of the ARCSI/heavy processing commands and create dummy output files.

There's quite a lot of setup needed for all the folders/inputs/configs. To get started quickly, you can copy the contents of the `local_dev` directory out to a test area and update the \<test_path> and \<code_path> values in the `test-luigi.cfg` and `run_local_test.sh` files. You should be able to just run the script after that with `./run_local_test.sh`.

## Luigi parameters

* `paths`
    * `/input` - A folder containing the input granule files as zip files directly downloaded from a Copernicus data source such as SciHub. If running with `--testProcessing`, these will need to be in the unzipped Mundi format (without '.SAFE' at the end of the folder name).
    * `/static` - A folder containing the static input files for this processing chain, two files need to exist here;
        - DEM file - a digital elevation model converted and stored as a KEA file in the required output projection format.
        - Projection WKT - a WKT representation of the request output projection as OGC WKT.
        The `metadata-config.json`, `arcsi_cmd_template.txt`, and `s2_metadata_template.xml` files can also be stored here for convenience, though full filepaths are used to reference these files so it's not required.
    * `/state` - A folder that will contain the state files for this job, this can be output at the end of the process.
    * `/output` - A folder that will contain the requested output files, converted to tif with thumbnails, metadata, etc...
* `dem` - Used by ARCSI.
* `outWkt` - Used by ARCSI (optional).
* `projAbbv` - Used by ARCSI (optional).
* `metadataConfigFile` - Contains info such as place name and DEM title to be used to populate the metadata template.
* `metadataTemplate` - Template populated with values from the metadataConfigFile, buildConfigFile, and other run specific values.
* `arcsiCmdTemplate` - The templated ARCSI cmd.
* `buildConfigFile` - Contains build information such as GDAL and docker image versions which are referenced in the metadata. For uncontainerised runs of the workflow you'll need to create this manually.


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
