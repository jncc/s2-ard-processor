import luigi
import subprocess
from luigi import LocalTarget
from process_s2_swath.CheckFileExists import CheckFileExists

class BuildPyramidsAndCalulateStats(luigi.Task):
    pathRoots = luigi.DictParameter()
    pyramidLevels = luigi.Parameter()
    inputFile = luigi.Parameter()

    def run(self):
        t = CheckFileExists(self.inputFile)
        yield t

        try:
            import rsgislib
            from rsgislib import imageutils
            
            # Looks like this function builds image pyramids AND populates stats
            rsgislib.imageutils.popImageStats(self.inputFile, True, 0., True)
        except:
            raise RuntimeError("Could not populate image stats using rsgislib function")

    def output(self):
        return LocalTarget(self.inputFile)