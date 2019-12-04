import json
import luigi
import os
import logging
import datetime
from string import Template
from functional import seq
from luigi import LocalTarget

class GenerateProductMetadata(luigi.ExternalTask):
    paths = luigi.DictParameter()
    inputProduct = luigi.DictParameter()
    metadataConfig = luigi.DictParameter()
    metadataTemplate = luigi.Parameter()

    def enforce_dd(self, in_data):
        in_data = str(in_data)
        if len(in_data) == 1:
            return "0" + in_data
        return in_data

    def getAquisitionDate(self, arcsiMetadata, appendTime=True):
        data = arcsiMetadata["AcquasitionInfo"]

        aquisition_date = ""

        aquisition_date += self.enforce_dd(data["Date"]["Year"]) + "-"
        aquisition_date += self.enforce_dd(data["Date"]["Month"]) + "-"
        aquisition_date += self.enforce_dd(data["Date"]["Day"])
        
        if appendTime:
            aquisition_date += "T" + self.enforce_dd(data["Time"]["Hour"]) + ":"
            aquisition_date += self.enforce_dd(data["Time"]["Minute"]) + ":"
            aquisition_date += self.enforce_dd(data["Time"]["Second"]) + "Z"

        return aquisition_date

    def getProcessingDate(self, arcsiMetadata):
        processing_date = ""

        data = arcsiMetadata["ProductsInfo"]

        processing_date += self.enforce_dd(data["ProcessDate"]["Year"]) + "-"
        processing_date += self.enforce_dd(data["ProcessDate"]["Month"]) + "-"
        processing_date += self.enforce_dd(data["ProcessDate"]["Day"]) + "T"
        processing_date += self.enforce_dd(data["ProcessTime"]["Hour"]) + ":"
        processing_date += self.enforce_dd(data["ProcessTime"]["Minute"]) + ":"
        processing_date += self.enforce_dd(data["ProcessTime"]["Second"]) + "Z"

        return processing_date

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
        
    def GenerateMetadata(self, arcsiMetadata):

        fileIdentifier = arcsiMetadata["FileInfo"]["SREF_DOS_IMG_WHOLE"][:-4]
        dateToday = str(datetime.date.today())
        boundingBox = self.getBoundingBox(arcsiMetadata)
        processingDate = self.getProcessingDate(arcsiMetadata)
        aquisitionDate = self.getAquisitionDate(arcsiMetadata)
        publishedDate = self.getAquisitionDate(arcsiMetadata, appendTime=False)
        arcsiCloudCover = arcsiMetadata['ProductsInfo']['ARCSI_CLOUD_COVER']
        arcsiAotRangeMax = arcsiMetadata['ProductsInfo']['ARCSI_AOT_RANGE_MAX']
        arcsiAotRangeMin = arcsiMetadata['ProductsInfo']['ARCSI_AOT_RANGE_MIN']
        arcsiAotValue = arcsiMetadata['ProductsInfo']['ARCSI_AOT_VALUE']
        arcsiLutElevationMax = arcsiMetadata['ProductsInfo']['ARCSI_LUT_ELEVATION_MAX']
        arcsiLutElevationMin = arcsiMetadata['ProductsInfo']['ARCSI_LUT_ELEVATION_MIN']
        arcsiVersion = self.metadata["arcsiVersion"]
        projection = self.metadata ["projection"]
        referenceSystemCodeSpace = self.metadata ["targetSrs"].split(":")[0]
        referenceSystemCode = self.metadata ["targetSrs"].split(":")[1]
        demTitle = self.metadata ["demTitle"]
        placeName = self.metadata ["placeName"]
        parentPlaceName = self.metadata ["parentPlaceName"]

        metadataParams = {
            "fileIdentifier": fileIdentifier,
            "metadataDate": processingDate,
            "publishedDate": publishedDate,
            "extentWestBound": boundingBox["west"],
            "extentEastBound": boundingBox["east"],
            "extentSouthBound": boundingBox["south"],
            "extentNorthBound": boundingBox["north"],
            "extentStartDate": aquisitionDate,
            "extentEndDate": aquisitionDate,
            "arcsiCloudCover": arcsiCloudCover,
            "arcsiAotRangeMax": arcsiAotRangeMax,
            "arcsiAotRangeMin": arcsiAotRangeMin,
            "arcsiAotValue" : arcsiAotValue,
            "arcsiLutElevationMax" : arcsiLutElevationMax,
            "arcsiLutElevationMin" : arcsiLutElevationMin,
            "arcsiVersion" : arcsiVersion,
            "datasetVersion": "v1.0",
            "projection": projection,
            "referenceSystemCodeSpace": referenceSystemCodeSpace,
            "referenceSystemCode": referenceSystemCode,
            "demTitle": demTitle,
            "placeName": placeName,
            "parentPlaceName": parentPlaceName
        }


        template = Template(self.metadataTemplate)

        ardMetadata = template.substitute(metadataParams)
        
        metadataFileName = "%s_meta.xml" % fileIdentifier

        target = os.path.join(self.paths["working"], metadataFileName)

        with open(target, 'w') as out:
            out.write(ardMetadata)
            
        return target

    def run(self):
       
        arcsiMetadataFile = seq(inputProduct["files"]) \
                    .where(lambda x: x.endswith("meta.json")) \
                    .first()

        arcsiMetadata = {}
        with open(arcsiMetadataFile, "r") as mf:
            arcsiMetadata = json.load(mf)

        metadataFile = self.GenerateMetadata(arcsiMetadata)

        output = {
            "productName": self.inputProduct["productName"],
            "files": [metadataFile]
        }

        with self.output().open('w') as o:
            json.dump(output, o)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'GenerateProductMetadata_%s.json' % self.inputProduct["productName"])
        return LocalTarget(outFile)