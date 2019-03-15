import luigi
import os
import subprocess
import logging
import json
import process_s2_swath.common as common
from luigi import LocalTarget
from luigi.util import inherits
from process_s2_swath.GetInputFileInfos import GetInputFileInfos
from process_s2_swath.GetSatelliteAndOrbitNumber import GetSatelliteAndOrbitNumber
from process_s2_swath.CheckFileExists import CheckFileExists

log = logging.getLogger('luigi-interface')

@requires(GetInputFileInfos, GetSatelliteAndOrbitNumber)
class BuildFileList(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        cmd = "arcsibuildmultifilelists.py --input {} --header \"*MTD*.xml\" -d 3 -s sen2 --output {}" \
            .format(
                self.pathRoots["extracted"],
                os.path.join(self.pathRoots["temp"], "File_")
            )

        command_line_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True)
        # todo: logging probably doesn't work
        process_output, _ =  command_line_process.communicate()
        log.info(process_output)

        fileListPath = os.path.join(self.pathRoots["temp"], self.getOutputFileName())
        yield CheckFileExists(filePath=fileListPath)

        output = {
            "fileListPath": fileListPath
        }

        with self.output().open('w') as o:
            o.write(common.getFormattedJson(output))

    def getOutputFileName(self):
        with self.input()[0].open('r') as inputFileInfosFile, \
            self.input()[1].open('r') as satelliteAndOrbitNoFile:
            getInputFileInfosOutput = json.loads(inputFileInfosFile.read())
            getSatelliteAndOrbitNoOutput = json.loads(satelliteAndOrbitNoFile.read())

        basename = "File_Sentinel"
        satelliteLetter = getSatelliteAndOrbitNoOutput["satelliteNumber"]
        date = getInputFileInfosOutput["products"][0]["date"] # date should be the same for all

        return basename + satelliteLetter + "_" + date + ".txt"

    def output(self):
        outFile = os.path.join(self.pathRoots['state'], "BuildFileList.json")
        return LocalTarget(outFile)

