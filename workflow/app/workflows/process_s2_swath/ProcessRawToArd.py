import luigi
import os
import json
import subprocess
import logging
import glob
import .common as common
from luigi import LocalTarget
from luigi.util import requires
from .BuildFileList import BuildFileList
from .GetInputFileInfos import GetSwathInfo
from .GetSatelliteAndOrbitNumber import GetSatelliteAndOrbitNumber
from .CheckFileExistsWithPattern import CheckFileExistsWithPattern

log = logging.getLogger("luigi-interface")

@requires(BuildFileList, GetSwathInfo, GetSatelliteAndOrbitNumber)
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
    projectionOptions = luigi.DictParameter()
    outWkt = luigi.Parameter()
    projAbbv = luigi.Parameter()



    def getBaseNameFromFilename(self, filename):
        return "SEN2_%s_*_%s_ORB%s_*%s" % \
            (
                product["date"],
                product["tileId"],
                satelliteAndOrbitNoOutput["orbitNumber"],
                self.projAbbv
            )

    def getExpectedProductFilePatterns(self, outDir):
        swathInfo = {}
        satelliteAndOrbitNoOutput = {}
        with self.input()[1].open('r') as swathInfoFile, \
            self.input()[2].open('r') as satelliteAndOrbitNoFile:
            swathInfo = json.load(swathInfoFile)
            satelliteAndOrbitNoOutput = json.load(satelliteAndOrbitNoFile)

        expectedProducts = {
            "products": []
        }

        for product in swathInfo["products"]:
            expected = {
                "productName": product["productName"],
                "files": []
            }


            basename = "SEN2_%s_*_%s_ORB%s_*%s" % \
                (
                    product["date"],
                    product["tileId"],
                    satelliteAndOrbitNoOutput["orbitNumber"],
                    self.projectionAbbreviation
                )

            basename = os.path.join(outDir, basename)

            expected["files"].append(basename + "clouds.kea")
            expected["files"].append(basename + "meta.json")
            expected["files"].append(basename + "sat.kea")
            expected["files"].append(basename + "toposhad.kea")
            expected["files"].append(basename + "valid.kea")
            expected["files"].append(basename + "vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.kea")
            expected["files"].append(basename + "vmsk_sharp_rad_srefdem_stdsref.kea")

            expectedProducts["products"].append(expected)
        
        return expectedProducts

    def run(self):
        # Create / cleanout output directory
        tempOutdir = os.path.join(self.paths["temp"], "output")
        common.createDirectory(tempOutdir)

        buildFileListOutput = {}
        with self.input()[0].open('r') as buildFileListFile:
            buildFileListOutput = json.load(buildFileListFile)

        demFilePath = os.path.join(self.paths["static"], self.dem)
        projectionWktPath = os.path.join(self.paths["static"], self.projectionOptions["wkt"])
        fileListPath = buildFileListOutput["fileListPath"]

        cmd = "arcsi.py -s sen2 --stats -f KEA --fullimgouts -p RAD SHARP SATURATE CLOUDS TOPOSHADOW STDSREF DOSAOTSGL METADATA \
            --interpresamp near --interp cubic -t {} -o {} --projabbv {} --outwkt {} --dem {} \
            -k clouds.kea meta.json sat.kea toposhad.kea valid.kea stdsref.kea --multi -i {}" \
            .format(
                self.paths["temp"],
                tempOutdir,
                self.projAbbv,
                self.outWkt,
                demFilePath,
                fileListPath
            )

        expectedProducts = self.getExpectedProductFilePatterns(tempOutdir)
        if not self.testProcessing:
            try:
                log.info("Running cmd: " + cmd)
                subprocess.check_output(cmd, shell=True) 
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
                    testFilepath = os.path.join(tempOutdir, testFilename)

                    if not os.path.exists(testFilepath):
                        with open(testFilepath, "w") as testFile:
                            testFile.write("TEST")

        with self.output().open('w') as o:
            json.dump(expectedProducts, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'ProcessRawToArd.json')
        return LocalTarget(outFile)