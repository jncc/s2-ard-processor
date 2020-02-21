import json
import logging
import luigi
import os
import subprocess
from luigi import LocalTarget
from luigi.util import requires
from functional import seq
from pebble import ProcessPool, ProcessExpired

log = logging.getLogger('luigi-interface')

class ValidateCOG(luigi.Task):
    """
    Run validate_cloud_optimized_geotiff.py on the given geotiff
    }
    """
    paths = luigi.DictParameter()
    product = luigi.DictParameter()
    maxCogProcesses = luigi.IntParameter()
    validateCogScriptDir = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)

    def validateCogFile(self, tifFile):
        result = {
            "file": tifFile,
            "validCog": False
        }
        validateCogScriptPath = os.path.join(self.validateCogScriptDir, "validate_cloud_optimized_geotiff.py")
        cmd = "python {} {}".format(validateCogScriptPath, tifFile)

        if not self.testProcessing:
            self.executeSubProcess(cmd)

        result["validCog"] = True

        return result

    def executeSubProcess(self, cmd):
        try:
            log.info("Running cmd: " + cmd)

            subprocess.run(cmd, check=True, stderr=subprocess.STDOUT, shell=True)

        except subprocess.CalledProcessError as e:
            errStr = "command '{}' returned with error (code {}): {}".format(e.cmd, e.returncode, e.output)
            log.error(errStr)
            raise RuntimeError(errStr)

    def run(self):
    
        tifFiles = seq(self.product["files"]) \
                    .filter(lambda x: os.path.splitext(x)[1] == '.tif') \
                    .to_list()

        validateResults = []

        #Process multiple files simultaneously
        with ProcessPool(max_workers=self.maxCogProcesses) as pool:

            generateValidateJobs = pool.map(self.validateCogFile, tifFiles)

            try:
                for validateResult in generateValidateJobs.result():
                    validateResults.append(validateResult)
            except ProcessExpired as error:
                log.error("%s. Exit code: %d" % (error, error.exitcode))

        output = {
            "productName" : self.product["productName"],
            "files" : validateResults
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], "ValidateCOG_{}.json".format(self.product["productName"]))
        return LocalTarget(outFile)