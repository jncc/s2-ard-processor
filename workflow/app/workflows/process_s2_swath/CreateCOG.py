import luigi
import os
import subprocess
import logging
import json
from functional import seq
from luigi import LocalTarget
from pebble import ProcessPool, ProcessExpired

log = logging.getLogger("luigi-interface")

class CreateCOG(luigi.Task):
    """
    Takes in an input KEA file and converts it into a cloud optimised GeoTIFF using 
    """
    paths = luigi.DictParameter()
    product = luigi.DictParameter()
    maxCogProcesses = luigi.IntParameter()

    def generateCogFile(self, keaFile):
        outputFile = "%s.tif" % os.path.splitext(keaFile)[0]

        cmd = "gdaladdo -r nearest {} 2 4 8 16 32 64 128 256 512".format(keaFile)
        
        self.executeSubProcess(cmd)

        
        cmd = "gdal_translate -co \"GTiff\" -co \"COMPRESS=DEFLATE\" -co \"BIGTIFF=YES\" -co \"TILED=YES\" -co \"BLOCKXSIZE=512\" -co \"BLOCKYSIZE=512\" --config GDAL_TIFF_OVR_BLOCKSIZE 512  -co \"COPY_SRC_OVERVIEWS=YES\" {} {}".format(
            keaFile,
            outputFile
        )

        self.executeSubProcess(cmd)

        return outputFile

    def executeSubProcess(self, cmd):
        try:
            subprocess.check_output(cmd, shell=True)

        except subprocess.CalledProcessError as e:
            errStr = "command '{}' returned with error (code {}): {}".format(e.cmd, e.returncode, e.output)
            log.error(errStr)
            raise RuntimeError(errStr)

    def run(self):

        keaFiles = seq(self.product["files"]) \
                    .filter(lambda x: os.path.splitext(x)[1] == '.kea') \
                    .to_list()

        output = {
            "productName" : self.product["productName"],
            "files" : []
        }

        #Process multiple Kea files simultaneously
        with ProcessPool(max_workers=self.maxCogProcesses) as pool:

            generateCogJobs = pool.map(self.generateCogFile, keaFiles)

            try:
                for cogFile in generateCogJobs.result():
                    output["files"].append(cogFile)
            except ProcessExpired as error:
                log.error("%s. Exit code: %d" % (error, error.exitcode))

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], "{}_CreateCOG.json".format(self.product["productName"]))
        return LocalTarget(outFile)