import json
import luigi
import os
import logging
import datetime
from string import Template
from functional import seq
from luigi import LocalTarget

log = logging.getLogger("luigi-interface")

class GenerateProductMetadata(luigi.Task):
    paths = luigi.DictParameter()
    inputProduct = luigi.DictParameter()
    metadataConfig = luigi.DictParameter()
    buildConfig = luigi.DictParameter()
    metadataTemplate = luigi.Parameter()
    outputDir = luigi.Parameter()
    ardProductName = luigi.Parameter()
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
        
    def GenerateMetadata(self, arcsiMetadata):
        fileIdentifier = self.ardProductName
        dateToday = str(datetime.date.today())
        boundingBox = self.getBoundingBox(arcsiMetadata)
        processingDate = str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
        aquisitionDate = self.getAquisitionDate(arcsiMetadata)
        publishedDate = self.getAquisitionDate(arcsiMetadata)
        collectionTime = aquisitionDate.split("T")[1].split("Z")[0]
        arcsiCloudCover = arcsiMetadata['ProductsInfo']['ARCSI_CLOUD_COVER']
        arcsiAotRangeMax = arcsiMetadata['ProductsInfo']['ARCSI_AOT_RANGE_MAX']
        arcsiAotRangeMin = arcsiMetadata['ProductsInfo']['ARCSI_AOT_RANGE_MIN']
        arcsiAotValue = arcsiMetadata['ProductsInfo']['ARCSI_AOT_VALUE']
        arcsiLutElevationMax = arcsiMetadata['ProductsInfo']['ARCSI_LUT_ELEVATION_MAX']
        arcsiLutElevationMin = arcsiMetadata['ProductsInfo']['ARCSI_LUT_ELEVATION_MIN']
        arcsiVersion = self.buildConfig["arcsiVersion"]
        projection = self.metadataConfig["projection"]
        referenceSystemCodeSpace = self.metadataConfig["targetSrs"].split(":")[0]
        referenceSystemCode = self.metadataConfig["targetSrs"].split(":")[1]
        demTitle = self.metadataConfig["demTitle"]
        placeName = self.metadataConfig["placeName"]
        parentPlaceName = self.metadataConfig["parentPlaceName"]
        targetSrs = self.metadataConfig["targetSrs"]
        dockerImage = self.buildConfig["dockerImage"]
        gdalVersion = self.buildConfig["gdalVersion"]

        metadataParams = {
            "fileIdentifier": fileIdentifier,
            "title": fileIdentifier,
            "metadataDate": processingDate,
            "publishedDate": publishedDate,
            "extentWestBound": boundingBox["west"],
            "extentEastBound": boundingBox["east"],
            "extentSouthBound": boundingBox["south"],
            "extentNorthBound": boundingBox["north"],
            "collectionTime": collectionTime,
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
            "parentPlaceName": parentPlaceName,
            "targetSrs": targetSrs,
            "projection": projection,
            "dockerImage": dockerImage,
            "gdalVersion": gdalVersion
        }


        with open(self.metadataTemplate, 'r') as tf:
            template = Template(tf.read())

        ardMetadata = template.substitute(metadataParams)
        
        metadataFileName = "%s_meta.xml" % fileIdentifier

        target = os.path.join(self.outputDir, metadataFileName)

        with open(target, 'w') as out:
            out.write(ardMetadata)
            
        return target

    def run(self):
        arcsiMetadataFile = seq(self.inputProduct["files"]) \
            .where(lambda x: x.endswith("meta.json")) \
            .first()

        arcsiMetadata = {}

        if self.testProcessing:
            log.debug("Test Mode, Would load: {}".format(arcsiMetadataFile))
            with open("process_s2_swath/test/dummy-arcsi-metadata.json", "r") as mf:
                arcsiMetadata = json.load(mf)
        else:
            with open(arcsiMetadataFile, "r") as mf:
                arcsiMetadata = json.load(mf)

        metadataFile = self.GenerateMetadata(arcsiMetadata)

        output = {
            "productName": self.inputProduct["productName"],
            "files": [metadataFile]
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'GenerateProductMetadata_%s.json' % self.inputProduct["productName"])
        return LocalTarget(outFile)