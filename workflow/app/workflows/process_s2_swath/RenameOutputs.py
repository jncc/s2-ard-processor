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
from process_s2_swath.SplitGranuleHandler import SplitGranuleHandler

log = logging.getLogger('luigi-interface')

pp = pprint.PrettyPrinter(indent=4)

@requires(CreateCOGs, CreateThumbnails, GetSwathInfo, GetArcsiMetadata)
class RenameOutputs(luigi.Task):
    paths = luigi.DictParameter()
    oldFilenameDateThreshold = luigi.DateParameter()

    def useOldNamingConvention(self, products):
        # they should all be the same day so just use the first one
        acquisitionDate = products[0]["arcsiMetadataInfo"]["acquisitionDate"]

        return datetime.strptime(acquisitionDate, "%Y-%m-%dT%H:%M:%SZ").date() < self.oldFilenameDateThreshold

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
                "arcsiMetadataInfo": seq(arcsi["products"]).filter(lambda y: y["productName"] == x[0]).first()["arcsiMetadataInfo"]}) \
            .to_list()

        # Rename Files
        outputList = []

        if self.useOldNamingConvention(productList):
            spgHandler = SplitGranuleHandler()
            splitNames = spgHandler.getSplitGranuleNames(productList)

        for product in productList:
            ardProductName = ""
            fileBaseName = ""
            renamedFiles = []

            oldProductName = product["productName"]

            newProductName = ""
            if oldProductName in splitNames:
                newProductName = splitNames[oldProductName]
            else:
                newProductName = oldProductName
            newProductName = newProductName.replace("SEN2", product["satellite"])

            for filepath in product["files"]:
                filename = os.path.basename(filepath)
                newFilename = filename.replace(oldProductName, newProductName)
                newFilepath = filepath.replace(filename, newFilename)
                if os.path.exists(filepath):
                    os.rename(filepath, newFilepath)
                renamedFiles.append(newFilepath)

                vmskFileSuffix = "_vmsk_sharp_rad_srefdem_stdsref.tif"
                if filepath.endswith(vmskFileSuffix):
                    filename = os.path.basename(newFilepath)
                    ardProductName = os.path.splitext(filename)[0]
                    fileBaseName = filename.replace(vmskFileSuffix, "")

            outputProduct = {
                "productName" : product["productName"],
                "date" : product["date"],
                "files" : renamedFiles,
                "ardProductName": ardProductName,
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