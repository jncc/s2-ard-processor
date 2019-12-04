import luigi
import os
import json
import re
import process_s2_swath.common as common
from luigi import LocalTarget
from luigi.util import requires
from .UnzipRaw import UnzipRaw

@requires(UnzipRaw)
class GetSatelliteAndOrbitNumber(luigi.Task):
    """
    Extracts the satellite and orbit number of the incomming products (only 
    extractsthe first product in the list as they MUST all be the same)

    TODO: Enforce check that all files have the same satellite and orbit? Wrap
    this into getfileinfos task and then run a validation check there

    This returns an limited extraction of the raw product metadata in the form;
    
    {
        "satelliteNumber": "2B",
        "orbitNumber": "137"
    }
    """
    paths = luigi.DictParameter()

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

    def run(self):
        unzipRawInfo = {}
        with self.input().open('r') as i:
            unzipRawInfo = json.load(i)

        output = {
            "metadata": []
        }

        # details should be the same for all granules so take the first one
        manifestPath = self.getManifestFilepath(unzipRawInfo["products"][0])

        with open(manifestPath, 'r') as m:
            manifestString = m.read()

        satelliteNumber = self.getSatelliteNumber(manifestString)
        orbitNumber = self.getOrbitNumber(manifestString)

        output = {
            "satelliteNumber": satelliteNumber,
            "orbitNumber": orbitNumber
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'GetSatelliteAndOrbitNumber.json')
        return LocalTarget(outFile)
