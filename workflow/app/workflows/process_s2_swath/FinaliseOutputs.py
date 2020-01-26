import luigi
import os
import shutil
import json
import logging
import pprint

from datetime import datetime
from luigi import LocalTarget
from luigi.util import requires
from functional import seq
from process_s2_swath.GenerateMetadata import GenerateMetadata
from process_s2_swath.CreateCOGs import CreateCOGs
from process_s2_swath.GetSwathInfo import GetSwathInfo
from process_s2_swath.CreateThumbnails import CreateThumbnails
from process_s2_swath.common import clearFolder

log = logging.getLogger('luigi-interface')

pp = pprint.PrettyPrinter(indent=4)

@requires(GenerateMetadata, CreateCOGs, CreateThumbnails, GetSwathInfo)
class FinaliseOutputs(luigi.Task):
    """
    Cleanup and other work should go here
    """
    paths = luigi.DictParameter()
    removeInputFiles = luigi.BoolParameter(default = False)

    def run(self):
        # take the files we want to keep and move them to the output folder
        # files to keep: .tif, .json, and any of our metadata files 
        # e.g. http://gws-access.ceda.ac.uk/public/defra_eo/sentinel/2/processed/ard/SEPA/
        meta = {}
        cogs = {}
        thumbs = {}
        info = {}

        with self.input()[0].open('r') as gm, \
            self.input()[1].open('r') as c, \
            self.input()[2].open('r') as t, \
            self.input()[3].open('r') as i:

            meta = json.load(gm)
            cogs = json.load(c)
            thumbs = json.load(t)
            info = json.load(i)


        # Combine metadata and products 
        productList = seq(cogs["products"]) \
                    .map(lambda x: (x["productName"], x["files"])) \
                    .join(
                        seq(meta["products"]) \
                        .map(lambda x: (x["productName"], x["files"]))) \
                    .map(lambda x: (x[0], seq(x[1]).flatten())) \
                    .join(
                        seq(thumbs["products"]) \
                        .map(lambda x: (x["productName"], x["files"]))) \
                    .map(lambda x: {
                        "productName": x[0],
                        "files": seq(x[1]).flatten(),
                        "date": seq(info["products"]).filter(lambda y: y["productName"] == x[0]).first()["date"],
                        "tileId": seq(info["products"]).filter(lambda y: y["productName"] == x[0]).first()["tileId"],
                        "satellite": seq(info["products"]).filter(lambda y: y["productName"] == x[0]).first()["satellite"]}) \
                    .to_list()

        # Rename Files
        # TODO: logic here: EODS ard project -> processing/workflow/process_s2_ard.py - line 228
        # Move products to output
        log.info("Moving products to output folder {}".format(self.paths["output"]))
        outputList = []

        for product in productList:
            outputProduct = {
                "productName" : product["productName"],
                "date" : product["date"],
                "tileId" : product["tileId"],
                "files" : []
            }

            pDate =  datetime.strptime(product["date"],"%Y%m%d").date()
            outputPath = os.path.join(self.paths["output"], 
                str(pDate.year), 
                "{:02d}".format(pDate.month), 
                "{:02d}".format(pDate.day), 
                product["productName"])
            
            copyList = seq(product["files"]) \
                .map(lambda f: (f, f.replace(cogs["outputDir"], outputPath)
                    .replace("SEN", product["satellite"]))) \
                .to_list()

            for c in copyList:
                targetPath = os.path.dirname(c[1])
                
                if not os.path.exists(targetPath):
                    os.makedirs(targetPath)

                shutil.copy(c[0], c[1])

                outputProduct["files"].append(c[1])

            outputList.append(outputProduct)
                
        output = {"products": outputList}

        #empty out the working folder
        clearFolder(self.paths["working"])

        # cleanup input files if flag is set
        if self.removeInputFiles:
            clearFolder(self.paths["input"])


        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'FinaliseOutputs.json')
        return LocalTarget(outFile)