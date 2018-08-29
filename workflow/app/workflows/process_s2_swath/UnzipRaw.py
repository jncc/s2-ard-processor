import luigi
import os
import subprocess
import json
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.PrepareWorkingFolder import PrepareWorkingFolder

@requires(PrepareWorkingFolder)
class UnzipRaw(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        # runs arcsi extract
        cmd = "arcsiextractdata.py -i {} -o {}" \
            .format(
                self.pathRoots["input"],
                self.pathRoots["extracted"])

        subprocess.check_output(
            cmd,
            stderr=subprocess.STDOUT,
            shell=True)

        with self.output().open('w') as o:
            o.write('\{\}')
    
    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'UnzipRaw_SUCCESS.json')
        return LocalTarget(outFile)
    
