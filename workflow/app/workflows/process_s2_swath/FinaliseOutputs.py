import luigi
import os
from luigi import LocalTarget
from luigi.util import inherits
from process_s2_swath.OptimiseFiles import OptimiseFiles
from process_s2_swath.GenerateMetadata import GenerateMetadata
from process_s2_swath.GenerateThumbnails import GenerateThumbnails

@requires(OptimiseFiles, GenerateMetadata, GenerateThumbnails)
class FinaliseOutputs(luigi.Task):
    """
    Cleanup and other work should go here, or this is just a pseudo task at the
    end of the chain potentially?

    TODO: Need to work the sensor back into the naming convention at some point
    i.e. SEN2A... / SEN2B ....
    """
    pathRoots = luigi.DictParameter()

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