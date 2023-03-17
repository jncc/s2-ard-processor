# Sentinel 2 ARD workflow

## Requirements

* An ARCSI v4 installation/container
* Python 3.6+ (we use 3.10)
* A Digital Elevation Model in kea format, and output projection in wkt format (as required by ARCSI)
* A working area with folder setup like so:
  * `/input` containing your Sentinel-2 L1C inputs, e.g. S2B_MSIL1C_20220620T110629_N0400_R137_T30UXD_20220620T115229
  * `/output`
  * `/working`
  * `/static` containing your DEM, WKT, and metadata config file
  * `/state`
  * `/report` (only if reporting is needed)
  * `/database` (only if reporting is needed)

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

Where PYTHONPATH is the path to the process_s2_swath directory and LUIGI_CONFIG_PATH is the path to your luigi.cfg file. The FinaliseOutputs target task will run all the steps to produce the ARD.

We also have a target task for producing the ARD plus creating an SQLite database and csv files for reporting. Note that this will require some extra params as shown in the `luigi.cfg.template` file.

    PYTHONPATH='.' LUIGI_CONFIG_PATH='luigi.cfg' luigi --module process_s2_swath GenerateReport --local-scheduler

## Local dev

You can test the workflow logic without actually needing to install ARCSI by using the `--testProcessing` flag. This will skip most of the ARCSI/heavy processing commands and create dummy output files.

There's quite a lot of setup needed for all the folders/inputs/configs. **To get started quickly** you can copy the contents of the `local_dev` directory out to a test area and update the \<test_path> and \<code_path> values in the `test-luigi.cfg` and `run_local_test.sh` files. You should be able to just run the script after that with `./run_local_test.sh`.

## Luigi parameters

* **paths**
    * **/input** - Path to a folder containing the input granule files as zip files directly downloaded from a Copernicus data source such as SciHub. If running with `--testProcessing`, these will need to be in the unzipped Mundi format (without '.SAFE' at the end of the folder name).
    * **/static** - Path to a folder containing the static input files for this processing chain (mainly for convenience when running in a container). The following files need to exist here: the DEM, the projection WKT, and the metadata config JSON. The arcsi_cmd_template.txt and s2_metadata_template.xml files can also be stored here if custom ones are used.
    * **/state** - Path to a folder that will contain the state files for this job, this can be output at the end of the process.
    * **/output** - Path to a folder that will contain the requested output files, converted to tif with thumbnails, metadata, etc...
    * **/report** - Path to a folder that will contain the output csv reports.
    * **/database** - Path to a folder that will container the output database file for reporting.
* **dem** - A digital elevation model in the required output projection format, to be provided as a KEA filename, e.g. `dem.kea`. Used by ARCSI.
* **outWkt** (optional) - Output projection to be provided as a WKT file, e.g. `BritishNationalGrid.wkt`. Used by ARCSI.
* **projAbbv** (optional) - Abbreviation for the output projection which is appended to the output filenames, e.g. `osgb`. Used by ARCSI.
* **metadataConfigFile** - JSON file containing info such as place name and DEM title to be used to populate the metadata template, e.g. `metadata-config.json`. (See the example in the `process_s2_swath/templates` folder.)
* **metadataTemplate** - TXT file with a template to be populated with values from the metadataConfigFile, buildConfigFile, and other run specific values. E.g. `s2_metadata_template.xml`.
* **arcsiCmdTemplate** - TXT file with the templated ARCSI cmd, e.g. `arcsi_cmd_template.txt`.
* **buildConfigFile** - JSON file containing build information such as the docker image version which is referenced in the metadata. (See `s2-ard-processor/workflow/config/app/worksflows/build-config.json` for an example.) For uncontainerised runs of the workflow you'll need to create this manually.
* **oldFilenameDateThreshold** - Date in format YYYY-MM-DD. All products with an acquisition date on or after this date will use the new filename convention with the generation time included.
* **maxCogProcesses** - Specify the number of parallel processes to be used to process the outputs to COGs.
* **removeInputFiles** (optional) - Cleans up the files in the `/input` folder on successful completion.
* **validateCogs** (optional) - Include a step to validate the output files are valid Cloud Optimised Geotiffs.
* **reportFileName** - A csv filename for the report output, e.g. `report.csv`. Used for the GenerateReport task only.
* **dbFilename** - A db filename for the SQLite database output, e.g. `s2ardproducts.db`. Used for the GenerateReport task only.
* **dbConnectionTimeout** (optional) - Specifies how many seconds to wait for the database file to be unlocked (if the file already exists). If you have a large swath with multiple processes writing to the same database file, it may be useful to adjust this. Used for the GenerateReport task only.
* **testProcessing** (optional) - Used for local development to run a lightweight version of the workflow which doesn't require GDAL or ARCSI and produces dummy outputs.


## Task Dependencies

Each section (seperated by ------------) is a functional step or (set of steps that can be run concurrently) that depends on the task in the above section giving a sort of dependency graph

| Task                           | Spawns (one or more*)           |
|--------------------------------|---------------------------------|
| PrepareRawGranules             |                                 |
| GetGDALVersion                 |                                 |
|--------------------------------|---------------------------------|
| GetSwathInfo                   | GetGranuleInfo   *              |
| GetSatelliteAndOrbitNumber     |                                 |
|--------------------------------|---------------------------------|
| BuildFileList                  |                                 |
|--------------------------------|---------------------------------|
| PrepareArdProcessing           | CheckFileExists*                |
|--------------------------------|---------------------------------|
| ProcessRawToArd                |                                 |
|--------------------------------|---------------------------------|
| CheckArdProducts               |                                 |
|--------------------------------|---------------------------------|
| RescaleCloudProbabilities      |                                 |
| GetArcsiMetadata               |                                 |
|--------------------------------|---------------------------------|
| CreateCOGs                     | CreateCOG*                      |
|--------------------------------|---------------------------------|
| CreateThumbnails               |                                 |
|--------------------------------|---------------------------------|
| RenameOutputs                  |                                 |
|--------------------------------|---------------------------------|
| GenerateMetadata               | GenerateProductMetadata*?       |
|--------------------------------|---------------------------------|
| FinaliseOutputs                |                                 |
|--------------------------------|---------------------------------|
| GenerateReport                 |                                 |
