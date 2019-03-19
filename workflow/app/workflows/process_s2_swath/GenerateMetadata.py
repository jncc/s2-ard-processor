import json
import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.CheckFileExists import CheckFileExists
from process_s2_swath.GenerateProductMetadata import GenerateProductMetadata
from process_s2_swath.CheckOutputFilesExist import CheckOutputFilesExist

@requires(CheckOutputFilesExist)
class GenerateMetadata(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        with self.input().open("r") as outputFile:
            outputFileJson = json.read(outputFile)

            generateMetadataTasks = []

            # make metadata file/(s) per product?
            for output in outputFileJson["convertedFiles"]:
                # TODO: Generate metadata task?
                generateMetadataTasks.append(GenerateProductMetadata(pathRoots=self.pathRoots, inputProduct=output))

            yield generateMetadataTasks

            for task in generateMetadataTasks:
                # TODO: do something?

        with self.output().open('w') as o:
            outputFileJson["generatedMetadata"] = True # TODO: per product more fine grained handling?
            json.dump(outputFileJson, o)

    def output(self):
        # some loigc to determin actual arcsi filelist file name
        outFile = os.path.join(self.pathRoots['state'], 'generatedMetadata.json')
        return LocalTarget(outFile)