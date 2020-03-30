import luigi
import os
import csv
import json
import re

from luigi import LocalTarget
from luigi.util import requires

from process_s2_swath.FinaliseOutputs import FinaliseOutputs

@requires(FinaliseOutputs)
class GenerateReport(luigi.Task):

    paths = luigi.DictParameter()
    reportFileName = luigi.Parameter()

    def parseInputName(self, productName):
        pattern = re.compile("S2([AB])\_MSIL1C\_((20[0-9]{2})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2}))")
        
        m = pattern.search(productName)

        satellite = "SENTINEL2%s" % m.group(1)
        captureDate = "%s-%s-%s" % (m.group(3), m.group(4), m.group(5)) 
        captureTime = "%s:%s:%s" % (m.group(6), m.group(7), m.group(8)) 

        return [productName, satellite, captureDate, captureTime]

    def run(self):
        finaliseOutputsInfo = {}
        with self.input().open('r') as finaliseOutputs:
            finaliseOutputsInfo = json.load(finaliseOutputs)

        reportLines = []

        for product in finaliseOutputsInfo["products"]:
            reportLine = self.parseInputName(product["productName"])
            reportLine.append(product["fileBaseName"])

            reportLines.append(reportLine)

        reportFilePath = os.path.join(self.paths["report"], self.reportFileName)

        exists = os.path.isfile(reportFilePath)

        if exists:
            openMode = "a"
        else:
            openMode = "w"

        with open(reportFilePath, openMode, newline='') as csvFile: 
            writer = csv.writer(csvFile)

            if not exists:  
                writer.writerow(["ProductId", "Platform", "Capture Date", "Capture Time", "ARD ProductId"])

            for line in reportLines:
                writer.writerow(line)            

        with self.output().open("w") as outFile:
            outFile.write(json.dumps({
                "reportFilePath" : reportFilePath,
                "reportLines" : reportLines
            }))
            
    def output(self):
        outputFile = os.path.join(self.paths["state"], "GenerateReport.json")
        return LocalTarget(outputFile)