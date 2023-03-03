import luigi
import os
import json
import logging
import pprint

from datetime import datetime
from luigi import LocalTarget
from luigi.util import requires
from functional import seq
from process_s2_swath.CreateCOGs import CreateCOGs
from process_s2_swath.GetSwathInfo import GetSwathInfo
from process_s2_swath.GetArcsiMetadata import GetArcsiMetadata
from process_s2_swath.CreateThumbnails import CreateThumbnails
from process_s2_swath.OldFilenameHandler import OldFilenameHandler

log = logging.getLogger('luigi-interface')

pp = pprint.PrettyPrinter(indent=4)

@requires(CreateCOGs, CreateThumbnails, GetSwathInfo, GetArcsiMetadata)
class RenameOutputs(luigi.Task):
    paths = luigi.DictParameter()
    oldFilenameDateThreshold = luigi.DateParameter()

    def useOldNamingConvention(self, products):
        # they should all be the same day so just use the first one
        acquisitionDateString = products[0]["arcsiMetadataInfo"]["acquisitionDate"]
        acquisitionDate = datetime.strptime(acquisitionDateString, "%Y-%m-%dT%H:%M:%SZ").date()

        return acquisitionDate < self.oldFilenameDateThreshold

    def getProductNameFromFiles(self, files):
        vmskFileSuffix = "_vmsk_sharp_rad_srefdem_stdsref.tif"
        vmskFiles = [f for f in files if f.endswith(vmskFileSuffix)]
        
        if len(vmskFiles) > 1:
            raise Exception(f"Error, found more than one vmsk file: {vmskFiles}")

        basename = os.path.basename(vmskFiles[0])
        productName = basename.replace(vmskFileSuffix, "")

        return productName

    def run(self):
        cogs = {}
        thumbs = {}
        info = {}
        arcsi = {}

        with self.input()[0].open('r') as c, \
            self.input()[1].open('r') as t, \
            self.input()[2].open('r') as i, \
            self.input()[3].open('r') as a:

            cogs = json.load(c)
            thumbs = json.load(t)
            info = json.load(i)
            arcsi = json.load(a)

        # Combine metadata and products 
        productList = seq(cogs["products"]) \
            .map(lambda x: (x["productName"], x["files"])) \
            .join(
                seq(thumbs["products"]) \
                .map(lambda x: (x["productName"], x["files"]))) \
            .map(lambda x: {
                "productName": x[0],
                "files": seq(x[1]).flatten(),
                "date": seq(info["products"]).filter(lambda y: y["productName"] == x[0]).first()["date"],
                "satellite": seq(info["products"]).filter(lambda y: y["productName"] == x[0]).first()["satellite"],
                "ardProductName": self.getProductNameFromFiles(seq(x[1]).flatten()),
                "arcsiMetadataInfo": seq(arcsi["products"]).filter(lambda y: y["productName"] == x[0]).first()["arcsiMetadataInfo"]}) \
            .to_list()

        # Rename Files
        outputList = []

        oldNames = {}
        if self.useOldNamingConvention(productList):
            nameHandler = OldFilenameHandler()
            oldNames = nameHandler.getFilenamesUsingOldConvention(productList)

        for product in productList:
            renamedFiles = []

            oldArdName = product["ardProductName"]

            newArdName = ""
            if oldArdName in oldNames:
                newArdName = oldNames[oldArdName]
            else:
                newArdName = oldArdName
            newArdName = newArdName.replace("SEN2", product["satellite"])

            for filepath in product["files"]:
                filename = os.path.basename(filepath)
                newFilename = filename.replace(oldArdName, newArdName)
                newFilename = newFilename.replace("clouds_prob_rescaled.tif", "clouds_prob.tif")

                newFilepath = filepath.replace(filename, newFilename)
                
                if os.path.exists(filepath):
                    os.rename(filepath, newFilepath)
                renamedFiles.append(newFilepath)

            fileBaseName = self.getProductNameFromFiles(renamedFiles)

            outputProduct = {
                "productName" : product["productName"],
                "date" : product["date"],
                "files" : renamedFiles,
                "fileBaseName": fileBaseName
            }

            outputList.append(outputProduct)
                
        output = {
            "products": outputList,
            "outputDir" : cogs["outputDir"]
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'RenameOutputs.json')
        return LocalTarget(outFile)