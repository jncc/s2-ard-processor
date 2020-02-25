# Runtime environment

## static folder
- wkt file
- dem
- metadata-config.json file

{
    "projection" : "OSGB",
    "targetSrs" : "EPSG:27700",
    "demTitle" : "dem title",
    "placeName" : "United Kingdom",
    "parentPlaceName" : "Europe"
}

# If running MPI jobs
- jasmin-mpi-config.json file (in root of static folder)

{
    "container" : {
        "location" : "/img/path/imag.simg",
        "mounts" : [
            ("/host/path","/container/path"),
            ("/host/path2","/container/path2")
        ]
    }
    "jobTemplate" : "s2_mpi_job_template.bsub"
}

- mpi-template.bsub


## Runtime parameters
--metadataTemplate - can be overridden with a template path relative to the container - ie /working/templates/mytemplate.xml

