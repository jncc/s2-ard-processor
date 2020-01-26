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
        tileId = self.getTileId(productName)
        date = self.getDate(productName)
        satellite = self.getSatellite(productName)

        output = {
            "productPath": self.productPath,
            "productName": productName,
            "date": date,
            "tileId": tileId,
            "satellite": satellite
        }

        with self.output().open('w') as o:
            json.dump(output,o,indent=4)

    def getTileId(self, productName):
        return productName[38:44]

    def getDate(self, productName):
        return productName[11:19]

    def getSatellite(self, productName):
        return productName[0:3]

    def output(self):
        filename = "GetGranuleInfo_{}.json".format(os.path.basename(self.productPath))
        outFile = os.path.join(self.paths['state'], filename)
        return LocalTarget(outFile)