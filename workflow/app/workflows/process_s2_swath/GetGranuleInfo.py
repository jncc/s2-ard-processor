import luigi
import os
import json
import xml.etree.ElementTree as ET
from luigi import LocalTarget
from process_s2_swath.CheckFileExists import CheckFileExists

class GetGranuleInfo(luigi.Task):
    """
    For each extracted raw product return some information about that product from the 
    name and metadata files

    {
        "productPath": "/app/extracted/S2B_MSIL1C_20190226T111049_N0207_R137_T30UXD_20190226T163538",
        "productName": "S2B_MSIL1C_20190226T111049_N0207_R137_T30UXD_20190226T163538",
        "date": "20190226",
        "tileId": "T30UXD",
        "satellite": "S2B",
        "angles": {
            "sunAngles": {
                "zenith": "64.074871646887",
                "azimuth": "173.186498411533"
            },
            "viewingAngles": {
                "0": {
                    "zenith": "",
                    "azimuth": ""
                },
                "1": {
                    "zenith": "",
                    "azimuth": ""
                },
                "2": {
                    "zenith": "",
                    "azimuth": ""
                },
                ...
            }
        }
    }
    """

    paths = luigi.DictParameter()
    productPath = luigi.Parameter()

    def getTileMetadataPath(self, manifestPath):
        manifestTree = ET.parse(manifestPath)
        manifestRoot = manifestTree.getroot()
        tileMetadataRelativePath = manifestRoot.find('''.//dataObject[@ID='S2_Level-1C_Tile1_Metadata']/byteStream/fileLocation''').attrib['href']
        tileMetadataPath = os.path.join(self.productPath, tileMetadataRelativePath)

        return tileMetadataPath

    def getTileMetadataXmlRoot(self, path):
        tileMetadataTree = ET.parse(path)
        tileMetadataRoot = tileMetadataTree.getroot()

        return tileMetadataRoot

    def getSunAngles(self, root):
        zenith = root.find('''.//Mean_Sun_Angle/ZENITH_ANGLE''').text
        azimuth = root.find('''.//Mean_Sun_Angle/AZIMUTH_ANGLE''').text

        return {
            "zenith": zenith,
            "azimuth": azimuth
        }

    def getViewingAngles(self, root):
        viewingAngles = {}
        for viewingAngle in root.findall('''.//Mean_Viewing_Incidence_Angle'''):
            bandId = viewingAngle.get('bandId') # 0 indexed, ESA band ID
            zenith = viewingAngle.find('ZENITH_ANGLE').text
            azimuth = viewingAngle.find('AZIMUTH_ANGLE').text
            viewingAngles[bandId] = {
                "zenith": zenith,
                "azimuth": azimuth
            }

        return viewingAngles

    def run(self):
        manifestPath = os.path.join(self.productPath, "manifest.safe")
        yield CheckFileExists(filePath=manifestPath)

        tileMetadataPath = self.getTileMetadataPath(manifestPath)
        yield CheckFileExists(filePath=tileMetadataPath)

        root = self.getTileMetadataXmlRoot(tileMetadataPath)
        sunAngles = self.getSunAngles(root)
        viewingAngles = self.getViewingAngles(root)

        productName = os.path.basename(self.productPath)

        splits = productName.split("_")

        output = {
            "productPath": self.productPath,
            "productName": productName,
            "date": splits[2].split("T")[0],
            "tileId": splits[5],
            "satellite": splits[0],
            "angles": {
                "sunAngles": sunAngles,
                "viewingAngles": viewingAngles
            }
        }

        with self.output().open('w') as o:
            json.dump(output,o,indent=4)

    def output(self):
        filename = "GetGranuleInfo_{}.json".format(os.path.basename(self.productPath))
        outFile = os.path.join(self.paths['state'], filename)
        return LocalTarget(outFile)