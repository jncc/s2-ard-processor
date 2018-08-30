import luigi
import os
import subprocess
import logging
import json
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.UnzipRaw import UnzipRaw
from process_s2_swath.CheckFileExists import CheckFileExists

log = logging.getLogger('luigi-interface')

@requires(UnzipRaw)
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

        process_output, _ =  command_line_process.communicate()
        log.info(process_output)

        tasks = []
        fileListPath = os.path.join(self.pathRoots["temp"], self.getOutputFileName())
        tasks.append(CheckFileExists(filePath=fileListPath))
        yield tasks

        output = {
            "fileListPath": fileListPath
        }

        with self.output().open('w') as o:
            o.write(json.dumps(output))

    def getOutputFileName(self):
        products = os.listdir(self.pathRoots["extracted"])

        # potentially read this from the metadata instead
        basename = "File_Sentinel2"
        satelliteLetter = products[0][2:3]
        date = products[0][11:19]

        return basename + satelliteLetter + "_" + date + ".txt"

    def output(self):
        outFile = os.path.join(self.pathRoots['state'], "BuildFileList.json")
        return LocalTarget(outFile)

