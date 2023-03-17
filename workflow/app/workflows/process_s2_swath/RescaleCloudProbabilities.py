import luigi
import os
import subprocess
import logging
import json
from functional import seq
from luigi.util import requires
from luigi import LocalTarget
from process_s2_swath.common import writeBinaryFile
from process_s2_swath.CheckArdProducts import CheckArdProducts

log = logging.getLogger("luigi-interface")

@requires(CheckArdProducts)
class RescaleCloudProbabilities(luigi.Task):
    """
    Take the clouds_prob.kea files and use gdal_calc.py to scale them to 0-100 with type Byte and nodata of 255
    Command is pretty quick so let's do it all in this one task
    """
    paths = luigi.DictParameter()
    testProcessing = luigi.BoolParameter(default = False)

    def rescaleKeaFile(self, inputKea):
        outputKea = "%s_rescaled.kea" % os.path.splitext(inputKea)[0]

        cmd = f'gdal_calc.py -A {inputKea} --outfile {outputKea} --type Byte --calc="A/100"'
        
        if self.testProcessing:
            writeBinaryFile(outputKea)
        else:
            try:
                log.info("Running cmd: " + cmd)

                subprocess.run(cmd, check=True, stderr=subprocess.STDOUT, shell=True)

            except subprocess.CalledProcessError as e:
                errStr = "command '{}' returned with error (code {}): {}".format(e.cmd, e.returncode, e.output)
                raise RuntimeError(errStr)

        return outputKea

    def run(self):
        cloudProbEnding = "clouds_prob.kea"

        with self.input().open("r") as CheckArdProductsFile:
            ardProducts = json.load(CheckArdProductsFile)

        updatedProducts = []
        for product in ardProducts["products"]:
            cloudProbFile = seq(product["files"]).filter(lambda x: x.endswith(cloudProbEnding))[0]
            rescaledFile = self.rescaleKeaFile(cloudProbFile)

            updatedFileList = list(seq(product["files"]).map(lambda x: rescaledFile if x.endswith(cloudProbEnding) else x))

            updatedProducts.append({
                "productName" : product["productName"],
                "files" : updatedFileList
            })

        output = {
            "outputDir": ardProducts["outputDir"],
            "products": updatedProducts
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], "RescaleCloudProbabilities.json")
        return LocalTarget(outFile)