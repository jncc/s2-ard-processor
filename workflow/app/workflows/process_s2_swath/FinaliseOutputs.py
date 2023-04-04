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
from process_s2_swath.RenameOutputs import RenameOutputs
from process_s2_swath.common import clearFolder

log = logging.getLogger('luigi-interface')

pp = pprint.PrettyPrinter(indent=4)

@requires(GenerateMetadata, RenameOutputs)
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
        outputs = {}

        with self.input()[0].open('r') as gm, \
            self.input()[1].open('r') as ro:

            meta = json.load(gm)
            outputs = json.load(ro)


        # Combine metadata and products 
        productList = seq(outputs["products"]) \
            .map(lambda x: (x["productName"], x["files"])) \
            .join(
                seq(meta["products"]) \
                .map(lambda x: (x["productName"], x["files"]))) \
            .map(lambda x: {
                "productName": x[0],
                "acquisitionDate": seq(outputs["products"]).filter(lambda y: y["productName"] == x[0]).first()["acquisitionDate"],
                "fileBaseName": seq(outputs["products"]).filter(lambda y: y["productName"] == x[0]).first()["fileBaseName"],
                "files": seq(x[1]).flatten()}) \
            .to_list()

        # TODO: logic here: EODS ard project -> processing/workflow/process_s2_ard.py - line 228
        # Move products to output
        log.info("Moving products to output folder {}".format(self.paths["output"]))
        outputList = []

        for product in productList:
            outputProduct = {
                "productName" : product["productName"],
                "acquisitionDate" : product["acquisitionDate"],
                "fileBaseName" : product["fileBaseName"],
                "files" : []
            }

            pDate =  datetime.strptime(product["acquisitionDate"],"%Y%m%d").date()
            outputPath = os.path.join(self.paths["output"], 
                str(pDate.year), 
                "{:02d}".format(pDate.month), 
                "{:02d}".format(pDate.day), 
                product["fileBaseName"])
            
            copyList = seq(product["files"]) \
                .map(lambda f: (f, f.replace(outputs["outputDir"], outputPath))) \
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