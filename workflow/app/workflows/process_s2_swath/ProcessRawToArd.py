import luigi
import os
import json
import subprocess
import logging
import process_s2_swath.common as common
from luigi import LocalTarget
from luigi.util import inherits
from process_s2_swath.BuildFileList import BuildFileList
from process_s2_swath.GetInputFileInfos import GetInputFileInfos
from process_s2_swath.GetSatelliteAndOrbitNumber import GetSatelliteAndOrbitNumber
from process_s2_swath.CheckFileExistsWithPattern import CheckFileExistsWithPattern

log = logging.getLogger("luigi-interface")

@requires(BuildFileList, GetInputFileInfos, GetSatelliteAndOrbitNumber)
class ProcessRawToArd(luigi.Task):
    pathRoots = luigi.DictParameter()
    dem = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)
    projwkt = luigi.Parameter()
    projabbv = luigi.Parameter(default = "osgb")
    
    def run(self):
        # Create / cleanout output directory
        common.createDirectory(self.pathRoots["output"])

        buildFileListOutput = {}
        with self.input()[0].open('r') as buildFileListFile:
            buildFileListOutput = json.loads(buildFileListFile.read())

        demFilePath = os.path.join(self.pathRoots["static"], self.dem)
        projectionWktPath = os.path.join(self.pathRoots["static"], self.projwkt)
        fileListPath = buildFileListOutput["fileListPath"]

        cmd = "arcsi.py -s sen2 --stats -f KEA --fullimgouts -p RAD SHARP SATURATE CLOUDS TOPOSHADOW STDSREF DOSAOTSGL METADATA \
            --interpresamp near --interp cubic -t {} -o {} --projabbv {} --outwkt {} --dem {} \
            -k clouds.kea meta.json sat.kea toposhad.kea valid.kea stdsref.kea --multi -i {}" \
            .format(
                self.pathRoots["temp"],
                self.pathRoots["output"],
                self.projabbv,
                projectionWktPath,
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
                    testFilepath = os.path.join(self.pathRoots["output"], testFilename)

                    if not os.path.exists(testFilepath):
                        with open(testFilepath, "w") as testFile:
                            testFile.write("TEST")

        tasks = []
        for product in expectedFilePatterns["products"]:
            for filePattern in product["files"]:
                tasks.append(CheckFileExistsWithPattern(dirPath=self.pathRoots["output"], pattern=filePattern))
        yield tasks

        output = {
            "files": []
        }

        for task in tasks:
            output["files"].append(task.output().fn)
            
        with self.output().open('w') as o:
            o.write(common.getFormattedJson(output))

    def getExpectedFilePatterns(self):
        inputFileInfosOutput = {}
        satelliteAndOrbitNoOutput = {}
        with self.input()[1].open('r') as inputFileInfosFile, \
            self.input()[2].open('r') as satelliteAndOrbitNoFile:
            inputFileInfosOutput = json.loads(inputFileInfosFile.read())
            satelliteAndOrbitNoOutput = json.loads(satelliteAndOrbitNoFile.read())

        expectedFiles = {
            "products": []
        }

        for product in inputFileInfosOutput["products"]:
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

    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'ProcessRawToArd.json')
        return LocalTarget(outFile)
