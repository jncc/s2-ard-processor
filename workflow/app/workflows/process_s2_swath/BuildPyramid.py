import luigi
from luigi import LocalTarget
from process_s2_swath.CheckFileExists import CheckFileExists

class BuildPyramid(luigi.Task):
    pathRoots = luigi.DictParameter()
    inputFile = luigi.Parameter()

    def run(self):
        t = CheckFileExists(self.inputFile)
        yield t
        # import os.path
        # import sys
        # import rsgislib
        # from rsgislib import imageutils

        # rsgislib.imageutils.popImageStats(os.path.join(inDir, file), True, 0., True)
        # generate output file name /file/path/name.tif

    def output(self):
        # outFile =output path/ generated output file name
        return LocalTarget(outFile)