import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.BuildPyramidsAndCalculateStats import BuildPyramidsAndCalculateStats

@requires(BuildPyramidsAndCalculateStats)
class CheckOutputFilesExist(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        # with self.input
        #  for each file
        #    create task to check file
        #  yield to tasks

        with self.output().open('w') as o:
            # write out input file list
            o.write('some files')

    def output(self):
        # some loigc to determin actual arcsi filelist file name

        outFile = os.path.join(self.pathRoots['state'], 'CheckOutputFilesExist.json')
        return LocalTarget(outFile)