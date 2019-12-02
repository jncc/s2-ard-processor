import luigi
import os
import json
import subprocess
import logging
import glob
import .common as common
from luigi import LocalTarget
from luigi.util import inherits
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
    pathRoots = luigi.DictParameter()
    dem = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)
    projectionOptions = luigi.DictParameter()
    outWkt = luigi.Parameter()
    projAbbv = luigi.Parameter()

    def checkFiles(self, dirPath, pattern):
        #TODO: Should this raise log
        filePattern = os.path.join(dirPath, pattern)
        matchingFiles = glob.glob(filePattern)
        result = True

        if not len(matchingFiles) == 1:
            log.error("ARD processing error, found more than one file for pattern " + pattern)
            result = False
        if not os.path.isfile(matchingFiles[0]):
            log.error("ARD processing error, " + matchingFiles[0] + " is not a file")
            result = False
        if not os.path.getsize(matchingFiles[0]) > 0:
            log.error("ARD processing error, file size is 0 for " + matchingFiles[0])
            result = False

        return result

    def getBaseNameFromFilename(self, filename):
        return "SEN2_%s_*_%s_ORB%s_*%s" % \
            (
                product["date"],
                product["tileId"],
                satelliteAndOrbitNoOutput["orbitNumber"],
                self.projAbbv
            )

    def getExpectedFilePatterns(self):
        swathInfo = {}
        satelliteAndOrbitNoOutput = {}
        with self.input()[1].open('r') as swathInfoFile, \
            self.input()[2].open('r') as satelliteAndOrbitNoFile:
            swathInfo = json.loads(swathInfoFile.read())
            satelliteAndOrbitNoOutput = json.loads(satelliteAndOrbitNoFile.read())

        expectedFiles = {
            "products": []
        }

        for product in swathInfo["products"]:
            productFiles = {
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
            productFiles["files"].append(basename + "clouds.kea")
            productFiles["files"].append(basename + "meta.json")
            productFiles["files"].append(basename + "sat.kea")
            productFiles["files"].append(basename + "toposhad.kea")
            productFiles["files"].append(basename + "valid.kea")
            productFiles["files"].append(basename + "vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.kea")
            productFiles["files"].append(basename + "vmsk_sharp_rad_srefdem_stdsref.kea")

            expectedFiles["products"].append(productFiles)
        
        return expectedFiles

    def run(self):
        # Create / cleanout output directory
        tempOutdir = os.path.join(self.pathRoots["temp"], "output")
        common.createDirectory(tempOutdir)

        buildFileListOutput = {}
        with self.input()[0].open('r') as buildFileListFile:
            buildFileListOutput = json.loads(buildFileListFile.read())

        demFilePath = os.path.join(self.pathRoots["static"], self.dem)
        projectionWktPath = os.path.join(self.pathRoots["static"], self.projectionOptions["wkt"])
        fileListPath = buildFileListOutput["fileListPath"]

        cmd = "arcsi.py -s sen2 --stats -f KEA --fullimgouts -p RAD SHARP SATURATE CLOUDS TOPOSHADOW STDSREF DOSAOTSGL METADATA \
            --interpresamp near --interp cubic -t {} -o {} --projabbv {} --outwkt {} --dem {} \
            -k clouds.kea meta.json sat.kea toposhad.kea valid.kea stdsref.kea --multi -i {}" \
            .format(
                self.pathRoots["temp"],
                tempOutdir,
                self.projAbbv,
                self.outWkt,
                demFilePath,
                fileListPath
            )

        expectedFilePatterns = self.getExpectedFilePatterns()
        if not self.testProcessing:
            try:
                log.info("Running cmd: " + cmd)
                subprocess.check_output(cmd, shell=True) 
            except subprocess.CalledProcessError as e:
                errStr = "command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output)
                log.error(errStr)
                raise RuntimeError(errStr)
        else:
            log.info("Generating mock output files")
            for product in expectedFilePatterns["products"]:
                for filePattern in product["files"]:
                    testFilename = filePattern.replace("*", "TEST")
                    testFilepath = os.path.join(tempOutdir, testFilename)

                    if not os.path.exists(testFilepath):
                        with open(testFilepath, "w") as testFile:
                            testFile.write("TEST")

        tasks = []
        fileCheck = True
        for product in expectedFilePatterns["products"]:
            for filePattern in product["files"]:
                if not (self.CheckFileExistsWithPattern(dirPath=os.path.join(tempOutdir, pattern=filePattern))): fileCheck = False

        if not fileCheck:
            raise Exception("Product Validation failed")
        
        # TODO: make this output sensible again? probably more in line with the ExpectedFilePatterns JSON object            
        with self.output().open('w') as o:
            json.dump(expectedFilePatterns,o, indent=4)

    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'ProcessRawToArd.json')
        return LocalTarget(outFile)
