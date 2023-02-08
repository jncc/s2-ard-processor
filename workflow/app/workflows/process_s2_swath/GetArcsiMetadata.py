import json
import luigi
import os
import logging
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.CheckArdProducts import CheckArdProducts

log = logging.getLogger("luigi-interface")

@requires(CheckArdProducts)
class GetArcsiMetadata(luigi.Task):
    paths = luigi.DictParameter()
    testProcessing = luigi.BoolParameter(default = False)

    def enforce_dd(self, in_data):
        in_data = str(in_data)
        if len(in_data) == 1:
            return "0" + in_data
        return in_data

    def getAquisitionDate(self, arcsiMetadata):
        data = arcsiMetadata["AcquasitionInfo"]

        aquisition_date = ""

        aquisition_date += self.enforce_dd(data["Date"]["Year"]) + "-"
        aquisition_date += self.enforce_dd(data["Date"]["Month"]) + "-"
        aquisition_date += self.enforce_dd(data["Date"]["Day"]) + "T"
        aquisition_date += self.enforce_dd(data["Time"]["Hour"]) + ":"
        aquisition_date += self.enforce_dd(data["Time"]["Minute"]) + ":"
        aquisition_date += self.enforce_dd(data["Time"]["Second"]) + "Z"

        return aquisition_date

    def getBoundingBox(self, arcsiMetadata):

        bboxSrc = arcsiMetadata["LocationInfo"]["Geographical"]["BBOX"]

        latValues = []
        latValues.append(bboxSrc["BLLat"])
        latValues.append(bboxSrc["BRLat"])
        latValues.append(bboxSrc["TLLat"])
        latValues.append(bboxSrc["TRLat"])       

        lonValues = []
        lonValues.append(bboxSrc["BLLon"])
        lonValues.append(bboxSrc["BRLon"])
        lonValues.append(bboxSrc["TLLon"])
        lonValues.append(bboxSrc["TRLon"]) 

        boundingBox = {
            "north": max(latValues),
            "south": min(latValues),
            "east": max(lonValues),
            "west": min(lonValues)
        }

        return boundingBox

    def getArcsiMetadataInfo(self, arcsiMetadata):
        arcsiMetadataInfo = {
            "boundingBox": self.getBoundingBox(arcsiMetadata),
            "acquisitionDate": self.getAquisitionDate(arcsiMetadata),
            "publishedDate": self.getAquisitionDate(arcsiMetadata),
            "arcsiCloudCover": arcsiMetadata['ProductsInfo']['ARCSI_CLOUD_COVER'],
            "arcsiAotRangeMax": arcsiMetadata['ProductsInfo']['ARCSI_AOT_RANGE_MAX'],
            "arcsiAotRangeMin": arcsiMetadata['ProductsInfo']['ARCSI_AOT_RANGE_MIN'],
            "arcsiAotValue": arcsiMetadata['ProductsInfo']['ARCSI_AOT_VALUE'],
            "arcsiLutElevationMax": arcsiMetadata['ProductsInfo']['ARCSI_LUT_ELEVATION_MAX'],
            "arcsiLutElevationMin": arcsiMetadata['ProductsInfo']['ARCSI_LUT_ELEVATION_MIN'],
            "arcsiVersion": arcsiMetadata['SoftwareInfo']['Version']
        }

        return arcsiMetadataInfo
        
    def run(self):
        ardProducts = {}
        with self.input().open('r') as checkArdProducts:
            ardProducts = json.load(checkArdProducts)

        output = {
            "products": []
        }
        for product in ardProducts["products"]:
            arcsiMetadataFile = list(filter(lambda f: f.endswith("meta.json"), product["files"]))[0]

            arcsiMetadataFileContents = {}
            if self.testProcessing:
                log.debug("Test Mode, Would load: {}".format(arcsiMetadataFile))
                with open("process_s2_swath/test/dummy-arcsi-metadata.json", "r") as mf:
                    arcsiMetadataFileContents = json.load(mf)
            else:
                with open(arcsiMetadataFile, "r") as mf:
                    arcsiMetadataFileContents = json.load(mf)

            productInfo = {
                "productName": product["productName"],
                "arcsiMetadataInfo": self.getArcsiMetadataInfo(arcsiMetadataFileContents)
            }

            output["products"].append(productInfo)

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'GetArcsiMetadata.json')
        return LocalTarget(outFile)