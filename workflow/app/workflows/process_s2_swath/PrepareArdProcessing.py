import luigi
import os
import json
import subprocess
import logging
import glob
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.common import createDirectory
from process_s2_swath.BuildFileList import BuildFileList
from process_s2_swath.GetSwathInfo import GetSwathInfo
from process_s2_swath.GetSatelliteAndOrbitNumber import GetSatelliteAndOrbitNumber
from process_s2_swath.CheckFileExistsWithPattern import CheckFileExistsWithPattern
from process_s2_swath.CheckFileExists import CheckFileExists

log = logging.getLogger("luigi-interface")

@requires(BuildFileList, GetSwathInfo, GetSatelliteAndOrbitNumber)
class PrepareArdProcessing(luigi.Task):
    paths = luigi.DictParameter()
    dem = luigi.Parameter()
    outWkt = luigi.OptionalParameter()

    def run(self):
        # Check dem, wkt exist
        demFilePath = os.path.join(self.paths["static"], self.dem)
        projectionWktPath = os.path.join(self.paths["static"], self.outWkt)

        checkTasks = []
        checkTasks.append(CheckFileExists(filePath=demFilePath))

        if self.outWkt != "":
            checkTasks.append(CheckFileExists(filePath=projectionWktPath))

        yield checkTasks

        # Create / cleanout output directory
        tempOutDir = os.path.join(self.paths["working"], "output")
        createDirectory(tempOutDir)

        with self.output().open('w') as o:
            json.dump("Ready for ARD processing", o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'PrepareArdProcessing.json')
        return LocalTarget(outFile)
