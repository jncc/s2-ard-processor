import luigi
import os, stat
import csv
import sqlite3
import json
import re

from datetime import datetime
from luigi import LocalTarget
from luigi.util import requires

from process_s2_swath.FinaliseOutputs import FinaliseOutputs

@requires(FinaliseOutputs)
class GenerateReport(luigi.Task):

    paths = luigi.DictParameter()
    reportFileName = luigi.Parameter()
    dbFileName = luigi.OptionalParameter(default=None)
    dbConnectionTimeout = luigi.IntParameter(default=60000)

    def parseInputName(self, productName):
        pattern = re.compile("S2([AB])_MSIL1C_((20[0-9]{2})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2}))_\w+_(R[0-9]{3})_(T\w+)_")
        
        m = pattern.search(productName)

        platform = "2%s" % m.group(1)
        captureDate = "%s-%s-%s" % (m.group(3), m.group(4), m.group(5)) 
        captureTime = "%s:%s:%s" % (m.group(6), m.group(7), m.group(8)) 

        relativeOrbit = m.group(9)
        tileNumber = m.group(10)

        return [productName, platform, relativeOrbit, tileNumber, captureDate, captureTime]

    def writeToCsv(self, reportLines, reportFilePath):
        exists = os.path.isfile(reportFilePath)

        if exists:
            openMode = "a"
        else:
            openMode = "w"

        with open(reportFilePath, openMode, newline='') as csvFile: 
            writer = csv.writer(csvFile)

            if not exists:  
                writer.writerow(["ProductId", "Platform", "Relative Orbit", "Tile Number", "Capture Date", "Capture Time", "ARD ProductId"])

            for line in reportLines:
                writer.writerow(line) 

    def writeToDb(self, reportLines, dbPath):
        conn = sqlite3.connect(dbPath, timeout=self.dbConnectionTimeout)

        c = conn.cursor()
        c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='s2ArdProducts' ''')

        if c.fetchone()[0] != 1: 
            c.execute('''CREATE TABLE s2ArdProducts
                        (productId text, platform text, relativeOrbit text, tileNumber text, captureDate text, captureTime text, ardProductId text, recordTimestamp text)''')
            
            conn.commit()

        sql = "INSERT INTO s2ArdProducts VALUES (?,?,?,?,?,?,?,?)"

        for line in reportLines:
            recordTimestamp = str(datetime.now())
            row = (line[0], line[1], line[2], line[3], line[4], line[5], line[6], recordTimestamp)

            c.execute(sql, row)

        conn.commit()
        conn.close()

    def run(self):
        finaliseOutputsInfo = {}
        with self.input().open('r') as finaliseOutputs:
            finaliseOutputsInfo = json.load(finaliseOutputs)

        reportLines = []

        for product in finaliseOutputsInfo["products"]:
            reportLine = self.parseInputName(product["productName"])
            reportLine.append(product["fileBaseName"])

            reportLines.append(reportLine)

        if self.dbFileName:
            dbFilePath = os.path.join(self.paths["database"], self.dbFileName)
            dbExists = os.path.exists(dbFilePath)

            self.writeToDb(reportLines, dbFilePath)

            #If the file has just been created make it user and group writable.
            if not dbExists:
                os.chmod(dbFilePath, stat.S_IREAD | stat.S_IWRITE | stat.S_IRGRP | stat.S_IWGRP)

        reportFilePath = os.path.join(self.paths["report"], self.reportFileName)
        self.writeToCsv(reportLines, reportFilePath)

        with self.output().open("w") as outFile:
            outFile.write(json.dumps({
                "reportFilePath" : reportFilePath,
                "reportLines" : reportLines
            }))
            
    def output(self):
        outputFile = os.path.join(self.paths["state"], "GenerateReport.json")
        return LocalTarget(outputFile)