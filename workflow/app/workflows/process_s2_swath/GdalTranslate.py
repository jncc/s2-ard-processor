import luigi
from luigi import LocalTarget
from process_s2_swath.CheckFileExists import CheckFileExists

class GdalTranslate(luigi.Task):
    pathRoots = luigi.DictParameter()
    inputFile = luigi.Parameter()

    def run(self):
        t = CheckFileExists(self.inputFile)
        yield t
        #           gdal_translate -of GTiff -co "COMPRESS=LZW" -co "TILED=YES" -co "BLOCKXSIZE=256" -co "BLOCKYSIZE=256" -co "BIGTIFF=YES" $f ${f%.*}.tif;
        # generate output file name /file/path/name.tif

    def output(self):
        # outFile =output path/ generated output file name
        return LocalTarget(outFile)