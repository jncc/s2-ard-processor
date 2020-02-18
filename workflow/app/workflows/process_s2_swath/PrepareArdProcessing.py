import luigi
import os
import json
import subprocess
import logging
import glob
from string import Template
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.common import createDirectory
from process_s2_swath.BuildFileList import BuildFileList
from process_s2_swath.GetSwathInfo import GetSwathInfo
from process_s2_swath.GetSatelliteAndOrbitNumber import GetSatelliteAndOrbitNumber
from process_s2_swath.CheckFileExistsWithPattern import CheckFileExistsWithPattern
from process_s2_swath.CheckFileExists import CheckFileExists

log = logging.getLogger("luigi-interface")

@requires(BuildFileList, GetSwathInfo, GetSatelliteAndOrbitNumber)
class PrepareArdProcessing(luigi.Task):
    paths = luigi.DictParameter()
    dem = luigi.Parameter()
    outWkt = luigi.OptionalParameter(default = None)
    projAbbv = luigi.OptionalParameter(default = None)
    arcsiCmdTemplate = luigi.Parameter()

    def getExpectedProductFilePatterns(self, outDir, satelliteAndOrbitNoOutput, swathInfo):
        expectedProducts = {
            "products": []
        }

        for product in swathInfo["products"]:
            expected = {
                "productName": product["productName"],
                "date" : product["date"],
                "tileId" : product["tileId"],
                "files": []
            }

            if self.projAbbv: 
                abv = self.projAbbv + "_"
            else:
                abv = ""
            
            basename = "SEN2_%s_*_%s_ORB%s_*_%s" % \
                (
                    product["date"],
                    product["tileId"],
                    satelliteAndOrbitNoOutput["orbitNumber"],
                    abv
                )

            basename = os.path.join(outDir, basename)

            expected["files"].append(basename + "clouds.kea")
            expected["files"].append(basename + "meta.json")
            expected["files"].append(basename + "sat.kea")
            expected["files"].append(basename + "toposhad.kea")
            expected["files"].append(basename + "valid.kea")
            expected["files"].append(basename + "vmsk_sharp_rad_srefdem_stdsref.kea")

            expectedProducts["products"].append(expected)
        
        return expectedProducts

    def run(self):
        # Generate expected products list
        buildFileListOutput = {}
        swathInfo = {}
        satelliteAndOrbitNoOutput = {}

        with self.input()[0].open('r') as buildFileListFile, \
            self.input()[1].open('r') as swathInfoFile, \
            self.input()[2].open('r') as satelliteAndOrbitNoFile:
            
            swathInfo = json.load(swathInfoFile)
            satelliteAndOrbitNoOutput = json.load(satelliteAndOrbitNoFile)
            buildFileListOutput = json.load(buildFileListFile)

        fileListPath = buildFileListOutput["fileListPath"]

        # Check dem, wkt exist
        demFilePath = os.path.join(self.paths["static"], self.dem)

        checkTasks = []
        checkTasks.append(CheckFileExists(filePath=demFilePath))

        projectionWktPath = ""
        if self.outWkt:
            projectionWktPath = os.path.join(self.paths["static"], self.outWkt)
            checkTasks.append(CheckFileExists(filePath=projectionWktPath))

        yield checkTasks

        # Create / cleanout output directory
        tempOutDir = os.path.join(self.paths["working"], "output")
        createDirectory(tempOutDir)

        outWktParam = ""
        if self.outWkt:
            outWktParam = "--outwkt {}".format(projectionWktPath)

        projAbbvParam = ""
        if self.projAbbv:
            projAbbvParam = "--projabbv {}".format(self.projAbbv)

        arcsiRunParams = {
            "tmpDir": self.paths["working"],
            "outDir": tempOutDir,
            "dem": demFilePath,
            "fileList": fileListPath,
            "outWkt": outWktParam,
            "projAbbv": projAbbvParam
        }

        with open(self.arcsiCmdTemplate, 'r') as tf:
            template = Template(tf.read())

        arcsiCmd = template.substitute(arcsiRunParams)

        expectedProducts = self.getExpectedProductFilePatterns(tempOutDir, satelliteAndOrbitNoOutput, swathInfo)

        output = {
            "arcsiCmd": arcsiCmd,
            "expectedProducts": expectedProducts,
            "tempOutDir": tempOutDir
        }
        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'PrepareArdProcessing.json')
        return LocalTarget(outFile)
