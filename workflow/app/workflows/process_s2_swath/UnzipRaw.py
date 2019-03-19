import luigi
import os
import subprocess
import json
import glob
import process_s2_swath.common as common
from luigi import LocalTarget
from luigi.util import requires

class UnzipRaw(luigi.Task):
    """
    Unzips all zip files inside a given folder using the arcsiextractdata.py command,
    creates a list of extracted products and outputs to the following standard;

    {
        "products": [
            "/app/extracted/S2B_MSIL1C_20190226T111049_N0207_R137_T30UXD_20190226T163538",
            "/app/extracted/S2B_MSIL1C_20190226T111049_N0207_R137_T31UCT_20190226T163538",
            "..."
        ]
    }
    """
    pathRoots = luigi.DictParameter()

    def run(self):
        # Create / cleanout extracted folder to store extracted zip files
        common.createDirectory(self.pathRoots['extracted'])))

        # Extract data to extracted folder
        cmd = "arcsiextractdata.py -i {} -o {}" \
            .format(
                self.pathRoots["input"],
                self.pathRoots["extracted"])

        subprocess.check_output(
            cmd,
            stderr=subprocess.STDOUT,
            shell=True)

        extractedProducts = glob.glob(os.path.join(self.pathRoots["extracted"], "*"))

        output = {
            "products": extractedProducts
        }

        with self.output().open('w') as o:
            json.dump(output, o)
    
    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'UnzipRaw.json')
        return LocalTarget(outFile)
    
