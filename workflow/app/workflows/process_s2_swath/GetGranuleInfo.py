import luigi
import os
import json
from luigi import LocalTarget

class GetGranuleInfo(luigi.Task):
    """
    For each extracted raw product return some basic information about that product extracted from the 
    product name in the form of;

    {
        "productPath": "/app/extracted/S2B_MSIL1C_20190226T111049_N0207_R137_T30UXD_20190226T163538",
        "productName": "S2B_MSIL1C_20190226T111049_N0207_R137_T30UXD_20190226T163538",
        "date": "20190226",
        "tileId": "T30UXD",
        "satalite": "S2B"
    }
    """

    paths = luigi.DictParameter()
    productPath = luigi.Parameter()

    def run(self):
        productName = os.path.basename(self.productPath)

        splits = productName.split("_")

        output = {
            "productPath": self.productPath,
            "productName": productName,
            "date": splits[2].split("T")[0],
            "tileId": splits[5],
            "satellite": splits[0]
        }

        with self.output().open('w') as o:
            json.dump(output,o,indent=4)

    def output(self):
        filename = "GetGranuleInfo_{}.json".format(os.path.basename(self.productPath))
        outFile = os.path.join(self.paths['state'], filename)
        return LocalTarget(outFile)