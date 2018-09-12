import luigi
import os
from luigi import LocalTarget
from luigi.util import inherits
from process_s2_swath.BuildPyramids import BuildPyramids
from process_s2_swath.GenerateMetadata import GenerateMetadata

@inherits(BuildPyramids)
@inherits(GenerateMetadata)
class TransferArdToOutput(luigi.Task):
    pathRoots = luigi.DictParameter()

    def requires(self):
        t = []
        t.append(self.clone(BuildPyramids))
        t.append(self.clone(GenerateMetadata))
        return t

    def run(self):
        # take the files we want to keep and move them to the output folder
        # files to keep: .tif, .json, and any of our metadata files 
        # e.g. http://gws-access.ceda.ac.uk/public/defra_eo/sentinel/2/processed/ard/SEPA/

        with self.output().open('w') as o:
            # write out input file list
            o.write('some files')

    def output(self):
        # some loigc to determin actual arcsi filelist file name

        outFile = os.path.join(self.pathRoots['state'], 'TransferArdToOutput.json')
        return LocalTarget(outFile)