import luigi
import os
import json
import subprocess
import logging
import glob
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.ProcessRawToArd import ProcessRawToArd

@requires(ProcessRawToArd)
class CheckArdProducts(luigi.Task):
    paths = luigi.DictParameter()

    def checkFileExists(self, filePattern):
        matchingFiles = glob.glob(filePattern)
        result = True

        if not len(matchingFiles) == 1:
            log.error("ARD processing error, found more than one file for pattern " + pattern)
            result = False
        if not os.path.isfile(matchingFiles[0]):
            log.error("ARD processing error, " + matchingFiles[0] + " is not a file")
            result = False
        if not os.path.getsize(matchingFiles[0]) > 0:
            log.error("ARD processing error, file size is 0 for " + matchingFiles[0])
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
                "files" : []
            }

            for filePattern in expectedProduct["files"]:
                fileName = self.checkFileExists(filePattern)
                # todo :size check
                if len(fileName) == 0:
                    fileCheck = False
                else: 
                    product["files"].append(fileName)
                
            products.append(product)
                    

        if not fileCheck:
            raise Exception("Product Validation failed")
    
        output = {
            "products" : products
        }
        
        with self.output().open('w') as o:
            json.dump(output, o, indent=4)
    
    def output(self):
        outFile = os.path.join(self.paths['state'], 'CheckArdProducts.json')
        return LocalTarget(outFile)


