# Sentinel-2 Analysis Ready Data Processor

## What is this?

This container provides a python luigi workflow that processes a swath of raw Sentinel 2 granules from ESA to a set of Analysis Ready Data (ARD) products utilising the [ARCSI](http://remotesensing.info/arcsi/) tool developed by Dr Pete Bunting.

It takes a set of raw inputs (ESA zip files or Mundi folders) and a Digital Elevation Model and produces ARD rasters, as well as a GEMINI 2.3 metadata XML file.

The luigi workflow can run standalone or with a luigi central scheduler.

This container derives from the [petebunting/au-eoed](https://hub.docker.com/r/petebunting/au-eoed) container that includes ARCSI and GDAL.

The source code for the s2-ard-processor container is on [github](https://github.com/jncc/s2-ard-processor). You can find more details on the workflow parameters and folder setup in the workflow [readme](https://github.com/jncc/s2-ard-processor/blob/master/workflow/app/workflows/README.md).

## Mount points

This ARD processor consumes and generates large amounts of data and this may require you to mount external file systems to account for this. For this reason there are a number of locations in the container file system that you may wish to mount externally.

* Input - This mount point should contain the raw data you will be processing.
* Static - This should contain the DEM you will be using for terrain adjustment, metadata config file and a template for the ARCSI command that is run. An optional wkt file if the products require reprojection.
* Working - Temporary files / paths created during processing. This folder is cleared at the end of each run unless you specify the --noClean switch.  The working data is written to a subfolder of the format <productId> where the date components are derived from the capture date of the source product. The product Id is also derived from the source product.
* Output - This folder wlll contain the output. The output is written to a subfolder of the format \<Year>/\<Month>/\<Day>/\<ARD product name> where the date components are derived from the capture date of the source product. The ARD product name is also derived from the input product data.
* State - The state files generated for each task in the luigi workflow. This is an optional mount generally used for debugging. State files are copied into a subfolder of output with the structure ../state/\<Year>/\<Month>/\<Day>/<productId> unless the --noStateCopy flag is specified

## Command line

The command line is of the format 

docker \<docker parameters> jncc/s2-ard-processor FinaliseOutputs \<luigi-parameters>

FinaliseOutputs is the luigi task that requires all processing steps to be run and moves the output files to the output folder.

## Example:

```
docker run -i -v /data/input:/input -v /data/output:/output -v /data/state:/state -v /data/static:/static -v data/working:/working jncc/s2-ard-processor FinaliseOutputs --dem=dem.kea --outWkt=outwkt.txt --projAbbv=osgb --metadataConfigFile=metadata.config.json --metadataTemplate=metadataTemplate.xml  --oldFilenameDateThreshold=2023-01-01 --noStateCopy --local-scheduler
```

## Workflow options

* --testProcessing - Test the workflow parameters only, don't run the processing

## Luigi options

These parameters are relevant to the luigi worker running inside the container: See [Luigi docs](https://luigi.readthedocs.io/en/stable/configuration.html#core) for more information a full list of relevant options

* --local-scheduler - Use a container specific scheduler - assumed if scheduler host isn't provided
* --scheduler-host CORE_SCHEDULER_HOST - Hostname of machine running remote scheduler
* --scheduler-port CORE_SCHEDULER_PORT - Port of remote scheduler API process
* --scheduler-url CORE_SCHEDULER_URL - Full path to remote scheduler

## MPI processing

By default the workflow will run ARCSI in non-MPI mode but MPI is also supported. You'll need to do two additional things to enable it:

* Have OpenMPI installed on your system
* Add an additional luigi parameter to use a different ARCSI cmd template: `--arcsiCmdTemplate=/app/workflows/process_s2_swath/templates/arcsimpi_cmd_template.txt` (Note this will use the built in arcsimpi.py command template but you can specify your own if needed.)

## Outputs

Following a successful run the output folder will contain the following file structure:

```
../output
2022
   └── 06
        └── 20
            └── S2B_20220620_latn527lonw0007_T30UXD_ORB137_20220620115229_utm30n_osgb
            ├── S2B_20220620_latn527lonw0007_T30UXD_ORB137_20220620115229_utm30n_osgb_clouds.tif
            ├── S2B_20220620_latn527lonw0007_T30UXD_ORB137_20220620115229_utm30n_osgb_clouds_prob.tif
            ├── S2B_20220620_latn527lonw0007_T30UXD_ORB137_20220620115229_utm30n_osgb_sat.tif
            ├── S2B_20220620_latn527lonw0007_T30UXD_ORB137_20220620115229_utm30n_osgb_toposhad.tif
            ├── S2B_20220620_latn527lonw0007_T30UXD_ORB137_20220620115229_utm30n_osgb_valid.tif
            ├── S2B_20220620_latn527lonw0007_T30UXD_ORB137_20220620115229_utm30n_osgb_vmsk_sharp_rad_srefdem_stdsref.tif
            ├── S2B_20220620_latn527lonw0007_T30UXD_ORB137_20220620115229_utm30n_osgb_vmsk_sharp_rad_srefdem_stdsref_meta.xml
            └── S2B_20220620_latn527lonw0007_T30UXD_ORB137_20220620115229_utm30n_osgb_vmsk_sharp_rad_srefdem_stdsref_thumbnail.jpg
```