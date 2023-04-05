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
        "productPath": "/working/extracted/S2B_MSIL1C_20190226T111049_N0207_R137_T30UXD_20190226T163538",
        "productName": "S2B_MSIL1C_20190226T111049_N0207_R137_T30UXD_20190226T163538",
        "date": "20190226",
        "datetime": "20190226T163538"
        "tileId": "T30UXD",
        "satellite": "S2B",
        "processingBaseline": "03.01",
        "productDOI": "n/a",
        "angles": {
            "sunAngles": {
                "zenith": "64.074871646887",
                "azimuth": "173.186498411533"
            },
            "viewingAngles": {
                "0": {
                    "zenith": "3.10603184811342",
                    "azimuth": "167.123809953701"
                },
                "1": {
                    "zenith": "2.45782752383328",
                    "azimuth": "172.418266414506"
                },
                "2": {
                    "zenith": "2.56705199332649",
                    "azimuth": "170.157831055565"
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

    def getXmlRoot(self, path):
        tree = ET.parse(path)
        root = tree.getroot()

        return root

    def getProcessingBaseline(self, root):
        return root.find('.//Product_Info/PROCESSING_BASELINE').text

    def getProductDOI(self, root):
        doiElement = root.find('.//Product_Info/PRODUCT_DOI')

        if doiElement is None:
            return 'n/a'
        else:
            return doiElement.text

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
        mtdPath = os.path.join(self.productPath, "MTD_MSIL1C.xml")
        yield CheckFileExists(filePath=mtdPath)

        mtdXmlRoot = self.getXmlRoot(mtdPath)
        processingBaseline = self.getProcessingBaseline(mtdXmlRoot)
        productDOI = self.getProductDOI(mtdXmlRoot)

        manifestPath = os.path.join(self.productPath, "manifest.safe")
        yield CheckFileExists(filePath=manifestPath)

        tileMetadataPath = self.getTileMetadataPath(manifestPath)
        yield CheckFileExists(filePath=tileMetadataPath)

        mtdXmlRoot = self.getXmlRoot(tileMetadataPath)
        sunAngles = self.getSunAngles(mtdXmlRoot)
        viewingAngles = self.getViewingAngles(mtdXmlRoot)

        productName = os.path.basename(self.productPath)

        splits = productName.split("_")

        output = {
            "productPath": self.productPath,
            "productName": productName,
            "acquisitionDate": splits[2].split("T")[0],
            "generationDatetime": splits[6],
            "tileId": splits[5],
            "satellite": splits[0],
            "processingBaseline": processingBaseline,
            "productDOI": productDOI,
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