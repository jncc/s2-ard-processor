# Runtime environment

## static folder
- wkt file
- dem
- metadata-config.json file

{
            "arcsiVersion": "v3.1.6",
            "projection" : "OSGB",
            "targetSrs" : "EPSG:27700",
            "demTitle" : "dem title",
            "placeName" : "United Kingdom",
            "parentPlaceName" : "Europe"
}

# If running MPI jobs
- jasmin-mpi-config.json file (in root of static folder)
** NOTE Paths relative to host.
{
    "hostWorkingPath" : "/the/path/mounted/to/working",
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

