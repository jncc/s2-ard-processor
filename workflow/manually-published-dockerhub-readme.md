## What is this?

This container provides a luigi workflow that processes a swath of raw Sentinel 2 granules from ESA to a set of Analysis Ready Data (ARD) product utilising the ARCSI tools developed by Dr Pete Bunting

[ARCSI](https://www.arcsi.remotesensing.info/)

It takes a set of raw input (ESA zip file or Mundi folder) and a DEM and produces ARD rasters, as well as a GEMINI 2.3 metadata XML file.

The luigi workflow can run standalone or with a luigi central scheduler.

This container derives from the [jncc/jncc-arcsi-base](https://hub.docker.com/repository/docker/jncc/arcsi-mpi-base) container that provides ARCSI and some additional elements for supporting MPI on JASMIN.

The source code for both containers is on [github](https://github.com/jncc/s2-ard-processor)

## Mount points

This ARD processor consumes and generates large amounts of data and this may require you to mount external file systems to account for this. For this reason there are a number of locations in the container file system that you may wish to mount externally.

* Input - This mount point should contain the raw data you will be processing.
* Static - This should contain the DE you will be using for terrain adjustment, metadata config file and a template for the Arcsi command that is run. An optional wkt file if the products require reprojection.
* Working - Temporary files / paths created during processing. This folder is cleared at the end of each run unless you specify the --noClean switch.  The working data is written to a subfolder of the format <productId> where the date components are derived from the capture date of the source product. The product Id is also derived from the source product.
* Output - This folder wlll contain the output. The output is written to a subfolder of the format <Year>/<Month>/<Day>/<ARD product name> where the date components are derived from the capture date of the source product. The ARD product name is also derived from the input product data.
* State - The state files generated for each task in the luigi workflow. This is an optional mount generally for debugging. State files are copied into a subfolder of output with the structure ../state/<Year>/<Month>/<Day>/<productId> unless the --noStateCopy flag is specified


## Command line

The command line is of the format 

docker <docker parameters> jncc/s1-ard-processor FinaliseOutputs <luigi-parameters>

FinaliseOutputs is the luigi task that requires all processing steps to be run and moves the output files to the output folder.

# Example:

```
docker run -i -v /data/input:/input -v /data/output:/output -v /data/state:/state -v /data/static:/static -v data/working:/working jncc/s1-ard-processor FinaliseOutputs --dem=dem.kea --outWkt=outwkt.txt --projAbbv=osgb --metadataConfigFile=metadata.config.json --metadataTemplate=metadataTemplate.xml  --local-scheduler
```

# Luigi options

These parameters are relevant to the luigi worker running inside the container: See [Luigi docs](https://luigi.readthedocs.io/en/stable/configuration.html#core) for more information a full list of relevant options

* --local-scheduler - Use a container specific scheduler - assumed if scheduler host isn't provided
* --scheduler-host CORE_SCHEDULER_HOST - Hostname of machine running remote scheduler
* --scheduler-port CORE_SCHEDULER_PORT - Port of remote scheduler API process
* --scheduler-url CORE_SCHEDULER_URL - Full path to remote scheduler

# Workflow options

* --testProcessing - Test the workflow parameters only, don't run the processing

## Outputs

Following a successful run the output folder will contain the following structure.

# Output Product Name


# Example Output

Processing the product S1A_IW_GRDH_1SDV_20180104T062254_20180104T062319_020001_02211F_A294.zip will give the following output:

    ../output
    ├── 2018
    │   └── 01
    │       └── 04
    │           └── S1A_20180104_154_desc_062254_062319_DV_Gamma-0_GB_OSGB_RCTK_SpkRL
    │               ├── S1A_20180104_154_desc_062254_062319_DV_Gamma-0_GB_OSGB_RCTK_SpkRL.tif
    │               ├── S1A_20180104_154_desc_062254_062319_DV_Gamma-0_GB_OSGB_RCTK_SpkRL.xml
    └── state
        └── S1A_20180104_062254_062319
            ├── AddMergedOverviews.json
            ├── CheckArdFilesExist.json
            ├── ConfigureProcessing.json
            ├── CopyInputFile.json
            ├── CutDEM.json
            ├── EnforceZip.json
            ├── GenerateMetadata.json
            ├── GetConfiguration.json
            ├── GetManifest.json
            ├── MergeBands.json
            ├── ModifyNoDataTif.json
            ├── ProcessRawToArd.json
            ├── ReprojectToTargetSrs.json
            ├── TransferFinalOutput.json
            └── VerifyWorkflowOutput.json
