import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.ConvertToTif import ConvertToTif

@requires(ConvertToTif)
class BuildPyramids(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):

# inDir = 'Outputs'

# files = [file for file in os.listdir(inDir) if file.endswith('.tif')]

# for file in files:   


#         for f in ./Outputs/*.kea;
#           tasks.append(BuildPyramid(pathRoots=self.PathRoots, inputFile=f))
#        yield to tasks

        # for t in tasks
        #   filePath = t.output().name
        # Create list of expected output files

        with self.output().open('w') as o:
            # write out generated file list
            o.write('some files')

    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'BuildPyramids.json')
        return LocalTarget(outFile)