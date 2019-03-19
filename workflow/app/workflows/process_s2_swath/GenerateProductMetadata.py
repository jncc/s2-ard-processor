import json
import luigi
import os
from luigi import LocalTarget

class GenerateProductMetadata(luigi.ExternalTask):
    pathRoots = luigi.DictParameter()
    inputProduct = luigi.Parameter()

    def run(self):
        # TODO: figure out the logic here are we altering the existing metdata, creating new metadata format, etc...?
        generatedMetadata = {}

        with self.output().open('w') as o:
            json.dump(generatedMetadata, o)

    def output(self):
        # TODO: some loigc to determin actual arcsi filelist file name
        # TODO: figure out if this is json or xml metadata now?
        outFile = os.path.join(os.path.join(self.pathRoots["ouput"], self.inputProduct), "{}_meta.json".format(self.inputProduct)) 
        return LocalTarget(outFile)