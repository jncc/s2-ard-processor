import luigi
import os
import json
import subprocess
import logging
import glob
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.common import createDirectory
from process_s2_swath.PrepareArdProcessing import PrepareArdProcessing

log = logging.getLogger("luigi-interface")

@requires(PrepareArdProcessing)
class ProcessRawToArd(luigi.Task):

    """
    Main processing task, takes the input file from the BuildFileList and 
    processes the raw data pointed to by that file as a single job to ensure
    there are no edge effects.

    DEM
    ---
    The DEM filename needs to be supplied as the `dem` parameter and the file
    itself needs to be in the defined `static` folder as a KEA file in the
    correct output projection (not required but beneficial during processing).

    Output Projection
    -----------------
    The project of the file can be modified by supplying a different projection
    represented by an OGC WKT file (filename as projectionOptions["wkt"] and that file in the 
    defined `static` folder) as `projectionOptions["wkt"]` and an abbreviation for that 
    projection as `projectionOptions["abbv"]`.

    Returns a list of files that have been output by the process (as KEA files,
    etc...) in the form of;

    TODO: finalize outputs here currently suggest;
    {
        "products": [
            {
                "productName": "SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb",
                "files": [
                    "/app/temp/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_clouds.kea",
                    "/app/temp/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_meta.json",
                    "/app/temp/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_sat.kea",
                    "/app/temp/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_toposhad.kea",
                    "/app/temp/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_valid.kea",
                    "/app/temp/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.kea",
                    "/app/temp/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_vmsk_sharp_rad_srefdem_stdsref.kea"
                ]
            },
            {
                "productName": "SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb",
                "files": [
                    "/app/temp/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_clouds.kea",
                    "/app/temp/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_meta.json",
                    "/app/temp/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_sat.kea",
                    "/app/temp/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_toposhad.kea",
                    "/app/temp/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_valid.kea",
                    "/app/temp/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.kea",
                    "/app/temp/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_vmsk_sharp_rad_srefdem_stdsref.kea"
                ]
            },
            ...
        ]
    }
    """
    paths = luigi.DictParameter()
    dem = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)
    outWkt = luigi.OptionalParameter()
    projAbbv = luigi.OptionalParameter()
    mpi = luigi.BoolParameter(default = False)
    mpirunCmd = luigi.OptionalParameter()

    def run(self):
        prepareArdProcessing = {}
        with self.input()[].open('r') as prepareArdProcessingInfo:
            prepareArdProcessing = json.load(prepareArdProcessingInfo)

        expectedProducts = prepareArdProcessing["expectedProducts"]
        
        a = "arcsi.py" if not self.mpi else "{} arcsimpi.py".format(self.mpirunCmd)
        b = " -s sen2 --stats -f KEA --fullimgouts -p RAD SHARP SATURATE CLOUDS TOPOSHADOW STDSREF DOSAOTSGL METADATA"
        c = "-k clouds.kea meta.json sat.kea toposhad.kea valid.kea stdsref.kea --multi --interpresamp near --interp cubic"
        d = "-t {} -o {} --dem {} -i {}" \
        .format(
            self.paths["working"],
            prepareArdProcessing["tempOutDir"],
            prepareArdProcessing["demFilePath"],
            prepareArdProcessing["fileListPath"]
        )

        cmd = "{} {} {} {}".format(a, b, c, d)

        if self.outWkt:
            cmd = cmd + " --outwkt {}".format(prepareArdProcessing["projectionWktPath"])

        if self.projAbbv:
            cmd = cmd + " --projabbv {}".format(self.projAbbv)

        if not self.testProcessing:
            try:
                log.info("Running cmd: " + cmd)

                subprocess.run(cmd, check=True, stderr=subprocess.STDOUT, shell=True)
                
            except subprocess.CalledProcessError as e:
                errStr = "command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output)
                log.error(errStr)
                raise RuntimeError(errStr)
        else:
            #TODO: this needs refactoring to an external command that creats mock files
            log.info("Generating mock output files")
            for expectedProduct in expectedProducts["products"]:
                for filePattern in expectedProduct["files"]:
                    testFilename = filePattern.replace("*", "TEST")
                    testFilepath = os.path.join(prepareArdProcessing["tempOutDir"], testFilename)

                    if not os.path.exists(testFilepath):
                        with open(testFilepath, "w") as testFile:
                            testFile.write("TEST")
                            
        expectedProducts["outputDir"] = prepareArdProcessing["tempOutDir"]

        with self.output().open('w') as o:
            json.dump(expectedProducts, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'ProcessRawToArd.json')
        return LocalTarget(outFile)
