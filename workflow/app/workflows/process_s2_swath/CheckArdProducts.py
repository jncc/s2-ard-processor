import luigi
import os
import json
import subprocess
import logging
import glob
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.ProcessRawToArd import ProcessRawToArd


log = logging.getLogger('luigi-interface')

@requires(ProcessRawToArd)
class CheckArdProducts(luigi.Task):
    paths = luigi.DictParameter()

    def checkFileExists(self, filePattern):
        matchingFiles = glob.glob(filePattern)
        result = True

        if len(matchingFiles) < 1:
            log.error("ARD processing error, did not find any matching files for pattern {}".format(filePattern))
            result = False 
        elif len(matchingFiles) > 1:
            log.error("ARD processing error, found more than one file for pattern {}".format(filePattern))
            result = False
        elif not os.path.isfile(matchingFiles[0]):
            log.error("ARD processing error, {} is not a file".format(matchingFiles[0]))
            result = False
        elif not os.path.getsize(matchingFiles[0]) > 0:
            log.error("ARD processing error, file size is 0 for {} ".format(matchingFiles[0]))
            result = False

        if result:
            return matchingFiles[0]
        else:
            return ""

    def run(self):        
        expectedProducts = {}
        with self.input().open('r') as praFile:
            expectedProducts = json.load(praFile)

        products = []
        fileCheck = True
        for expectedProduct in expectedProducts["products"]:
            product = {
                "productName" : expectedProduct["productName"],
                "date" : expectedProduct["date"],
                "tileId" : expectedProduct["tileId"],
                "files" : []
            }

            for filePattern in expectedProduct["files"]:
                filePath = self.checkFileExists(filePattern)
                # todo :size check
                if len(filePath) == 0:
                    fileCheck = False
                else: 
                    product["files"].append(filePath)
                
            products.append(product)
                    

        if not fileCheck:
            raise Exception("Product Validation failed")
    
        output = {
            "outputDir" : expectedProducts["outputDir"],
            "products" : products
        }
        
        with self.output().open('w') as o:
            json.dump(output, o, indent=4)
    
    def output(self):
        outFile = os.path.join(self.paths['state'], 'CheckArdProducts.json')
        return LocalTarget(outFile)


