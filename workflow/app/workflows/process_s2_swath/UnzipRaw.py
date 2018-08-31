import luigi
import os
import subprocess
import json
import process_s2_swath.common as common
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.PrepareWorkingFolder import PrepareWorkingFolder

@requires(PrepareWorkingFolder)
class UnzipRaw(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        cmd = "arcsiextractdata.py -i {} -o {}" \
            .format(
                self.pathRoots["input"],
                self.pathRoots["extracted"])

        subprocess.check_output(
            cmd,
            stderr=subprocess.STDOUT,
            shell=True)

        extractedProducts = os.listdir(self.pathRoots["extracted"])

        output = {
            "products": extractedProducts
        }

        with self.output().open('w') as o:
            o.write(common.getFormattedJson(output))
    
    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'UnzipRaw.json')
        return LocalTarget(outFile)
    
