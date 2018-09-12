import luigi
import os
import json
import process_s2_swath.common as common
from luigi import LocalTarget

class GetInputFileInfo(luigi.Task):
    pathRoots = luigi.DictParameter()
    productPath = luigi.Parameter()

    def run(self):
        productName = os.path.basename(self.productPath)
        tileId = self.getTileId(productName)
        date = self.getDate(productName)

        output = {
            "productPath": self.productPath,
            "productName": productName,
            "date": date,
            "tileId": tileId
        }

        with self.output().open('w') as o:
            o.write(common.getFormattedJson(output))

    def getTileId(self, productName):
        return productName[38:44]

    def getDate(self, productName):
        return productName[11:19]

    def output(self):
        filename = "GetInputFileInfo_" + os.path.basename(self.productPath) + ".json"
        outFile = os.path.join(self.pathRoots['state'], filename)
        return LocalTarget(outFile)