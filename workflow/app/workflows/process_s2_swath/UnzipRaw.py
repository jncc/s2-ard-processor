import luigi
import os
import shutil
import subprocess
import json
import glob
from luigi import LocalTarget
from luigi.util import requires
from .common import createDirectory

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
    paths = luigi.DictParameter()

    def run(self):
        # Create / cleanout extracted folder to store extracted zip files
        extractPath = os.path.join(self.paths['working'], "extracted")

        createDirectory(extractPath)

        # Extract data to extracted folder
        cmd = "arcsiextractdata.py -i {} -o {}" \
            .format(
                self.paths["input"],
                extractPath)

        subprocess.check_output(
            cmd,
            stderr=subprocess.STDOUT,
            shell=True)

        # Move any folders to extracted
        for f in [dI for dI in os.listdir(self.paths["input"]) if os.path.isdir(os.path.join(self.paths["input"],dI))]:
            src = os.path.join(self.paths["input"], f)
            dst = os.path.join(extractPath, f)
            shutil.copytree(src, dst)

        extractedProducts = glob.glob(os.path.join(extractPath, "*"))

        output = {
            "products": extractedProducts
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)
    
    def output(self):
        outFile = os.path.join(self.paths['state'], 'UnzipRaw.json')
        return LocalTarget(outFile)
    
