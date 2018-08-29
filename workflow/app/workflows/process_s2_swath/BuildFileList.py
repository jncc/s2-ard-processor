import luigi
import os
import subprocess
import logging
import json
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.UnzipRaw import UnzipRaw

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

    def getOutputFileName(self):
        products = os.listdir(self.pathRoots["extracted"])

        basename = "File_Sentinel2"
        satellite = products[0][2:3]
        date = products[0][11:19]

        return basename + satellite + "_" + date + ".txt"

    def output(self):
        outFile = os.path.join(self.pathRoots['state'], self.getOutputFileName())
        return LocalTarget(outFile)

