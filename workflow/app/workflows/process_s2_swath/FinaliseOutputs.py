import luigi
import os
import shutil
import json
from luigi import LocalTarget
from luigi.util import requires
from functional import seq
from .OptimiseFiles import OptimiseFiles
from .GenerateMetadata import GenerateMetadata
from .CreateCOGs import CreateCOGs

log = logging.getLogger('luigi-interface')

@requires(GenerateMetadata, CreateCOGs)
class FinaliseOutputs(luigi.Task):
    """
    Cleanup and other work should go here
    """
    pathRoots = luigi.DictParameter()

    def run(self):
        # take the files we want to keep and move them to the output folder
        # files to keep: .tif, .json, and any of our metadata files 
        # e.g. http://gws-access.ceda.ac.uk/public/defra_eo/sentinel/2/processed/ard/SEPA/
        meta = {}
        with self.input()[0].open('r') as gm:
            meta = json.load(gm)

        cogs = {}
        with self.input()[0].open('r') as c:
            cogs = json.load(c)

        # Combine metadata and products 
        productList = seq(cogs["products"]) \
                    .map(lambda x: (x["productName"], x["files"])) \
                    .join(
                        seq(meta["products"]) \
                        .map(lambda x: (x["productName"], x["files"]))) \
                    .map(lambda x: {"productName": x[0], "files": seq(x[1]).flatten()}) \
                    .to_list()

        # Rename Files
        # TODO: logic here: EODS ard project -> processing/workflow/process_s2_ard.py - line 228
        # Move products to output
        log.info("Moving products to output folder {}".format(self.pathRoots["output"]))

        outputList = []

        for product in productList:
            outputProduct = {
                "productName" : product["productName"],
                "files" : []
            }

            copyList = seq(product["files"]) \
                .map(lambda f: (f, f.replace(self.pathRoots["temp"], self.pathRoots["output"])))

            for c in copyList:
                targetPath = os.path.dirname(c[1])
                
                if not os.path.exists(targetPath):
                    os.makedirs(targetPath)

                shutil.copy(c[0], c[1])

                outputProduct["files"].append(c[1])

            outputList.append[outputProduct]
                
        output = {"products": outputList}

        with self.output().open('w') as o:
            json.dump(output, o)

    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'FinaliseOutputs.json')
        return LocalTarget(outFile)