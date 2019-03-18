# eo-s2-workflow
Sentinel 2 ARD workflow

# Running

To run the workflow, you need to have and be running in an existing Arcsi base 
container (or other environment with Arcsi installed), this process will be 
wrapped up in a docker container, and will require the following inputs;

PYTHONPATH='.' luigi --module process_s2_swath -outwkt osgb.wkt -projabbv osgb -dem dem.kea

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

| Task                           | Spawns (one or more*)      |
|--------------------------------|----------------------------|
| UnzipRaw                       |                            |
|--------------------------------|----------------------------|
| GetInputFileInfos              | GetInfputFileInfo*         |
| GetSatelliteAndOrbitNumber     |                            |
|--------------------------------|----------------------------|
| BuildFileList                  |                            |
|--------------------------------|----------------------------|
| ProcessRawToArd                | CheckFileExistsWithPattern*|
|--------------------------------|----------------------------|
| ConvertToTif                   | GdalTranslateKeaToTif*     |
|--------------------------------|----------------------------|
| BuildPyramidsAndCalculateStats | BuildPyramids*             |
|                                | CalculateStats*            |
|--------------------------------|----------------------------|
| CheckOutputFilesExist          | ?                          |
|--------------------------------|----------------------------|
| GenerateMetadata               |                            |
| GenerateThumbnails             | GenerateThumbnail*?        |
|--------------------------------|----------------------------|
| FinalizeOutputs                |                            |

