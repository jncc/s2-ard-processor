import json
import luigi
import os
import logging
import datetime
import process_s2_swath.Defaults as defaults
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
    granuleInfo = luigi.DictParameter()
    arcsiInfo = luigi.DictParameter()
    bandConfig = luigi.DictParameter(default = defaults.BandConfig)
    testProcessing = luigi.BoolParameter(default = False)

    def addAngleParams(self, metadataParams):
        metadataParams["Mean_Sun_Angle_Zenith"] = self.granuleInfo["angles"]["sunAngles"]["zenith"]
        metadataParams["Mean_Sun_Angle_Azimuth"] = self.granuleInfo["angles"]["sunAngles"]["azimuth"]

        for band in self.bandConfig.values():
            bandId = "{:02d}".format(band['bandNo'])
            metadataParams["MVIA_B{0}_Zenith".format(bandId)] = self.granuleInfo["angles"]["viewingAngles"][str(band["esaBandId"])]["zenith"]
            metadataParams["MVIA_B{0}_Azimuth".format(bandId)] = self.granuleInfo["angles"]["viewingAngles"][str(band["esaBandId"])]["azimuth"]

    def getEsaFilename(self, productName):
        esaFilename = ""
        if "SPLIT" in productName:
            # remove the SPLIT1 substring to get the original ESA filename
            splitIndex = productName.find("SPLIT")
            esaFilename = productName[:splitIndex] + productName[splitIndex+6:]
        else:
            esaFilename = productName

        return esaFilename

    def run(self):
        arcsiMetadata = self.arcsiInfo["arcsiMetadataInfo"]

        fileIdentifier = self.ardProductName
        boundingBox = arcsiMetadata["boundingBox"]
        processingDate = str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
        aquisitionDate = arcsiMetadata["acquisitionDate"]
        publishedDate = arcsiMetadata["acquisitionDate"]
        collectionTime = aquisitionDate.split("T")[1].split("Z")[0]
        esaFilename = self.getEsaFilename(self.inputProduct["productName"])
        processingBaseline = self.granuleInfo["processingBaseline"]
        productDOI = self.granuleInfo["productDOI"]
        arcsiCloudCover = arcsiMetadata["arcsiCloudCover"]
        arcsiAotRangeMax = arcsiMetadata["arcsiAotRangeMax"]
        arcsiAotRangeMin = arcsiMetadata["arcsiAotRangeMin"]
        arcsiAotValue = arcsiMetadata["arcsiAotValue"]
        arcsiLutElevationMax = arcsiMetadata["arcsiLutElevationMax"]
        arcsiLutElevationMin = arcsiMetadata["arcsiLutElevationMin"]
        arcsiVersion = arcsiMetadata["arcsiVersion"]
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
            "esaFilename": esaFilename,
            "processingBaseline": processingBaseline,
            "productDoi": productDOI,
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

        self.addAngleParams(metadataParams)

        with open(self.metadataTemplate, 'r') as tf:
            template = Template(tf.read())

        ardMetadata = template.substitute(metadataParams)
        
        metadataFileName = "%s_meta.xml" % fileIdentifier

        metadataFilepath = os.path.join(self.outputDir, metadataFileName)

        with open(metadataFilepath, 'w') as out:
            out.write(ardMetadata)

        output = {
            "productName": self.inputProduct["productName"],
            "files": [metadataFilepath]
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'GenerateProductMetadata_%s.json' % self.inputProduct["productName"])
        return LocalTarget(outFile)