import luigi
import os
import subprocess
from luigi import LocalTarget
from process_s2_swath.CheckFileExists import CheckFileExists

class GdalTranslateKeaToTif(luigi.Task):
    """
    Takes in an input KEA file and converts it into GeoTIFF using 
    gdal_translate, this process does however strip file optimisations in the
    KEA file, so we add some back in here (compression, inner tiling, ...) and
    add further optimisations later. The task takes in an input filepath and an
    output filepath to store the resulting file at.

    This tasks outputs a LocalTarget pointing to the created file
    """
    pathRoots = luigi.DictParameter()
    inputFile = luigi.Parameter()
    outputFile = luigi.Parameter()

    def run(self):
        checkFiletask = CheckFileExists(self.inputFile)
        yield checkFiletask

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