import luigi
import subprocess
from luigi import LocalTarget
from process_s2_swath.CheckFileExists import CheckFileExists

class BuildPyramid(luigi.Task):
    pathRoots = luigi.DictParameter()
    pyramidLevels = luigi.Parameter()
    inputFile = luigi.Parameter()

    def run(self):
        t = CheckFileExists(self.inputFile)
        yield t

        cmd = "gdaladdo {} {}".format(self.inputFile, self.pyramidLevels)
        command_line_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True
        )
        
        # TODO: logging probably doesn't work
        process_output, _ =  command_line_process.communicate()
        log.info(process_output)

    def output(self):
        return LocalTarget(self.inputFile)