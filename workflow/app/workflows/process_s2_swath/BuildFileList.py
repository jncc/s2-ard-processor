import luigi
import os
import subprocess
import logging
import json
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.common import createDirectory
from process_s2_swath.GetSwathInfo import GetSwathInfo
from process_s2_swath.GetSatelliteAndOrbitNumber import GetSatelliteAndOrbitNumber
from process_s2_swath.CheckFileExists import CheckFileExists
from process_s2_swath.UnzipRaw import UnzipRaw

log = logging.getLogger('luigi-interface')

@requires(GetSwathInfo, GetSatelliteAndOrbitNumber, UnzipRaw)
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
    paths = luigi.DictParameter()

    def getOutputFileName(self, satelliteAndOrbitNoInfo, swathInfo):
        basename = "File_Sentinel"
        satelliteLetter = satelliteAndOrbitNoInfo["satelliteNumber"]
        date = swathInfo["products"][0]["date"] # date should be the same for all

        return basename + satelliteLetter + "_" + date + ".txt"

    def run(self):
        with self.input()[0].open('r') as swathInfoFile, \
            self.input()[1].open('r') as satelliteAndOrbitNoFile, \
            self.input()[2].open('r') as unzipRawFile:
            swathInfo = json.load(swathInfoFile)
            satelliteAndOrbitNoInfo = json.load(satelliteAndOrbitNoFile)
            unzipRawInfo = json.load(unzipRawFile)

        # Create / cleanout temporary folder
        createDirectory(self.paths['working'])

        # Build filelist for processing
        cmd = "arcsibuildmultifilelists.py --input {} --header \"*MTD*.xml\" -d 3 -s sen2 --output {}" \
            .format(
                unzipRawInfo["extractedProductRoot"],
                os.path.join(self.paths["working"], "File_")
            )

        command_line_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True)
        # todo: logging probably doesn't work
        process_output, _ =  command_line_process.communicate()
        log.info(process_output)

        fileListPath = os.path.join(self.paths["working"], self.getOutputFileName(satelliteAndOrbitNoInfo, swathInfo))
        yield CheckFileExists(filePath=fileListPath)

        output = {
            "fileListPath": fileListPath
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)
            
    def output(self):
        outFile = os.path.join(self.paths['state'], "BuildFileList.json")
        return LocalTarget(outFile)

