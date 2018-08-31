import luigi
import os
import json
import re
import process_s2_swath.common as common
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.UnzipRaw import UnzipRaw

@requires(UnzipRaw)
class ReadManifests(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        with self.input().open('r') as i:
            unzipRawOutput = json.loads(i.read())

        output = {
            "metadata": []
        }

        for product in unzipRawOutput["products"]:
            manifestPath = self.getManifestFilepath(product)

            with open(manifestPath, 'r') as m:
                manifestString = m.read()

            satelliteNumber = self.getSatelliteNumber(manifestString)
            date = self.getDate(product)
            tileId = self.getTileId(product)
            orbitNumber = self.getOrbitNumber(manifestString)

            metadata = {
                # the satellite number, orbit, and date *should* be the same for all products so can maybe move those out
                "product": product,
                "satelliteNumber": satelliteNumber,
                "date": date,
                "tileId": tileId,
                "orbitNumber": orbitNumber
            }

            output["metadata"].append(metadata)

        with self.output().open('w') as o:
            o.write(common.getFormattedJson(output))

    def getManifestFilepath(self, productName):
        productDirPath = os.path.join(self.pathRoots["extracted"], productName)
        productSafeName = os.listdir(productDirPath)[0]

        productSafePath = os.path.join(productDirPath, productSafeName)
        manifestPath = os.path.join(productSafePath, "manifest.safe")

        return manifestPath

    def getTileId(self, productName):
        return productName[38:44]

    def getDate(self, productName):
        return productName[11:19]

    def getOrbitNumber(self, manifestString):
        pattern = "<safe:relativeOrbitNumber type=\"start\">(.+)<\/safe:relativeOrbitNumber>"
        orbitNo = re.search(pattern, manifestString).group(1)
        return orbitNo

    def getSatelliteNumber(self, manifestString):
        pattern = "<safe:number>(.+)<\/safe:number>"
        satelliteNo = re.search(pattern, manifestString).group(1)
        return satelliteNo
    
    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'ReadManifest.json')
        return LocalTarget(outFile)