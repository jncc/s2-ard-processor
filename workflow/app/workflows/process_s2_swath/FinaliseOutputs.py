import luigi
import os
import shutil
import json
import logging
import pprint
from luigi import LocalTarget
from luigi.util import requires
from functional import seq
from process_s2_swath.GenerateMetadata import GenerateMetadata
from process_s2_swath.CreateCOGs import CreateCOGs
from process_s2_swath.common import clearFolder

log = logging.getLogger('luigi-interface')

@requires(GenerateMetadata, CreateCOGs)
class FinaliseOutputs(luigi.Task):
    """
    Cleanup and other work should go here
    """
    paths = luigi.DictParameter()

    def run(self):
        # take the files we want to keep and move them to the output folder
        # files to keep: .tif, .json, and any of our metadata files 
        # e.g. http://gws-access.ceda.ac.uk/public/defra_eo/sentinel/2/processed/ard/SEPA/
        meta = {}
        with self.input()[0].open('r') as gm:
            meta = json.load(gm)

        cogs = {}
        with self.input()[1].open('r') as c:
            cogs = json.load(c)

        # Combine metadata and products 
        productList = seq(cogs["products"]) \
                    .map(lambda x: (x["productName"], x["files"], x["date"], x["tileId"])) \
                    .join(
                        seq(meta["products"]) \
                        .map(lambda x: (x["productName"], x["files"]))) \
                    .map(lambda x: {"productName": x[0], "files": seq(x[1]).flatten(), "date": x[2], "tileId": x[3]}) \
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

            #Todo: move file to folder with structure based on start date as YYYY/MM/DD
            pDate =  datetime.strptime(product["date"],"%Y%m%d").date()
            outputPath = os.path.join(self.paths["output"], pDate.year, "{:02d}".format(pDate.month), "{:02d}".format(pDate.day), product["productName"])
            
            copyList = seq(product["files"]) \
                .map(lambda f: (f, f.replace(cogs["outputDir"], outputPath))) \
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

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'FinaliseOutputs.json')
        return LocalTarget(outFile)