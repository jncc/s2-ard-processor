import luigi
import os
import subprocess
import logging
import json
from luigi import LocalTarget

log = logging.getLogger("luigi-interface")

class GetGDALVersion(luigi.Task):
    paths = luigi.DictParameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        gdalVersion = ""

        cmd = "ogrinfo --version | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+'"
        if self.testProcessing:
            gdalVersion = 'X.X.X'
        else:
            try:
                log.info("Running cmd: " + cmd)
                cmdOutput = subprocess.run(cmd, check=True, shell=True, capture_output=True, text=True).stdout
                gdalVersion = cmdOutput.strip()
            except subprocess.CalledProcessError as e:
                errStr = "command '{}' returned with error (code {}): {}".format(e.cmd, e.returncode, e.output)
                raise RuntimeError(errStr)

        output = {
            "gdalVersion" : gdalVersion
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], "GetGDALVersion.json")
        return LocalTarget(outFile)