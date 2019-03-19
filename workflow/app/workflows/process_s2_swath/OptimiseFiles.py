import json
import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.BuildPyramidsAndCalulateStats import BuildPyramidsAndCalulateStats
from process_s2_swath.CalculateStats import CalculateStats
from process_s2_swath.ConvertToTif import ConvertToTif


@requires(ConvertToTif)
class OptimiseFiles(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):

        with self.input().open("r") as convertToTifFile:
            convertToTifJson = json.load(convertToTifFile)

            optimiseTasks = []

            for filename in convertToTifJson["convertedFiles"]:
                optimiseTasks.append(BuildPyramidsAndCalulateStats(pathRoots=self.pathRoots, inputFile=filename))

            yield optimiseTasks

        with self.output().open('w') as o:
            convertToTifJson["builtPyramids"] = True
            convertToTifJson["calculatedStats"] = True
            json.dump(convertToTifJson)

    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'OptimiseFiles.json')
        return LocalTarget(outFile)