import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.CheckOutputFilesExist import CheckOutputFilesExist

@requires(CheckOutputFilesExist)
class GenerateMetadata(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        # make metadata file

        with self.output().open('w') as o:
            # write out input file list
            o.write('some files')

    def output(self):
        # some loigc to determin actual arcsi filelist file name

        outFile = os.path.join(self.pathRoots['state'], 'metadata.xml')
        return LocalTarget(outFile)