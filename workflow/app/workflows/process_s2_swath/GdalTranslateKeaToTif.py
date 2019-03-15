import luigi
import os
import subprocess
from luigi import LocalTarget
from process_s2_swath.CheckFileExists import CheckFileExists

class GdalTranslateKeaToTif(luigi.Task):
    pathRoots = luigi.DictParameter()
    inputFile = luigi.Parameter()

    outputFile = ""

    def run(self):
        checkFiletask = CheckFileExists(self.inputFile)
        yield checkFiletask

        self.outputFile = os.path.splitext(self.inputFile)[0]
        cmd = "gdal_translate -of GTiff -co \"COMPRESS=LZW\" -co \"TILED=YES\" -co \"BLOCKXSIZE=256\" -co \"BLOCKYSIZE=256\" -co \"BIGTIFF=YES\" {} {}".format(
            self.inputFile, 
            self.outputFile
        )

        command_line_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True
        )
        
        # TODO: logging probably doesn't work
        process_output, _ =  command_line_process.communicate()
        log.info(process_output)

        checkOutputFileTask = CheckFileExists(self.outputFile)
        yield checkOutputFileTask

    def output(self):
        return LocalTarget(self.outputFile)