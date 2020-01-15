import luigi
import os
import subprocess
import logging
import json
from functional import seq
from luigi import LocalTarget
from pebble import ProcessPool, ProcessExpired
from process_s2_swath.CreateCOGs import CreateCOGs
from process_s2_swath.common import writeBinaryFile 


class CreateThumbnails(luigi.Task):

    maxCogProcesses = luigi.IntParameter()
    testProcessing = luigi.BoolParameter(default = False)

    def generateThumbnail(self, source):
        outputFile = "%s_thumbnail.jpg" % os.path.splitext(source[1])[0]

        cmd = "gdal_translate -b 3 -b 2 -b 1 -ot Byte -of JPEG -outsize 5%% 5%% {} {}".format(source[1], outputFile)

        if self.testProcessing:
            writeBinaryFile(outputFile)
        else:
            try:
                log.info("Running cmd: " + cmd)

                subprocess.run(cmd, check=True, stderr=subprocess.STDOUT, shell=True)

            except subprocess.CalledProcessError as e:
                errStr = "command '{}' returned with error (code {}): {}".format(e.cmd, e.returncode, e.output)
                log.error(errStr)
                raise RuntimeError(errStr)

        return {
            "productName" : source[0],
            "files": [outputFile]
        }


    def run(self):
        cogs = {}

        with self.input().open('r') as CreateCOGsFile:
            cogs = json.load(CreateCOGsFile)

        sources = seq(x["products"]) \
                    .map(lambda x: (x["productName"], 
                        seq(x["files"]).filter(lambda x: x.endswith("_vmsk_sharp_rad_srefdem_stdsref.tif"))[0])) \
                    .to_list()

        thumbnails = []

        #Process multiple Kea files simultaneously
        with ProcessPool(max_workers=self.maxCogProcesses) as pool:

            generateThumbnails = pool.map(self.generateThumbnail, sources)

            try:
                for thumb in generateThumbnails.result():
                    thumbnails.append(thumb)
            except ProcessExpired as error:
                log.error("%s. Exit code: %d" % (error, error.exitcode))

        
        output = {
            "outputDir": cogs["outputDir"],
            "products": thumbnails
            }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], "CreateThumbnails.json"))
        return LocalTarget(outFile)