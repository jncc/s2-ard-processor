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
    """
    Builds files lists for arcsi to process using the arcsibuildmultifilelists.py command, it
    iterates over the extracted folder to find all products that can be processed as one and passes
    them to the the next step to be processed, the output of this step will give a single file, 
    which is the config for the ProcessToArd step, an example of this is shown below;

    {
        "fileListPath": "/app/temp/File_Sentinel2B_137_20190226.txt"
    }
    """
    pathRoots = luigi.DictParameter()

    def run(self):
        # Create / cleanout temporary folder
        common.createDirectory(self.pathRoots['temp'])

        # Build filelist for processing
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

