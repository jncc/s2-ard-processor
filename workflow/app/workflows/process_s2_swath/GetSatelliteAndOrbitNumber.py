import luigi
import os
import json
import re
import process_s2_swath.common as common
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.UnzipRaw import UnzipRaw

@requires(UnzipRaw)
class GetSatelliteAndOrbitNumber(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        with self.input().open('r') as i:
            unzipRawOutput = json.loads(i.read())

        output = {
            "metadata": []
        }

        # details should be the same for all granules so take the first one
        manifestPath = self.getManifestFilepath(unzipRawOutput["products"][0])

        with open(manifestPath, 'r') as m:
            manifestString = m.read()

        satelliteNumber = self.getSatelliteNumber(manifestString)
        orbitNumber = self.getOrbitNumber(manifestString)

        output = {
            "satelliteNumber": satelliteNumber,
            "orbitNumber": orbitNumber
        }

        with self.output().open('w') as o:
            o.write(common.getFormattedJson(output))

    def getManifestFilepath(self, productPath):
        productSafeName = os.listdir(productPath)[0]

        productSafePath = os.path.join(productPath, productSafeName)
        manifestPath = os.path.join(productSafePath, "manifest.safe")

        return manifestPath

    def getOrbitNumber(self, manifestString):
        pattern = "<safe:relativeOrbitNumber type=\"start\">(.+)<\/safe:relativeOrbitNumber>"
        orbitNo = re.search(pattern, manifestString).group(1)
        return orbitNo

    def getSatelliteNumber(self, manifestString):
        pattern = "<safe:number>(.+)<\/safe:number>"
        satelliteNo = re.search(pattern, manifestString).group(1)
        return satelliteNo
    
    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'GetSatelliteAndOrbitNumber.json')
        return LocalTarget(outFile)