import luigi
import os
from luigi import LocalTarget
from process_s2_swath.CheckFileExists import CheckFileExists

class GdalTranslateKeaToTif(luigi.Task):
    pathRoots = luigi.DictParameter()
    inputFile = luigi.Parameter()

    def run(self):
        checkFiletask = CheckFileExists(self.inputFile)
        yield checkFiletask
        #           gdal_translate -of GTiff -co "COMPRESS=LZW" -co "TILED=YES" -co "BLOCKXSIZE=256" -co "BLOCKYSIZE=256" -co "BIGTIFF=YES" $f ${f%.*}.tif;
        # generate output file name /file/path/name.tif

    def output(self):
        # outFile =output path/ generated output file name
        return LocalTarget(outFile)