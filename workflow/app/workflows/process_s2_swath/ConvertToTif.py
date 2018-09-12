import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.ProcessRawToArd import ProcessRawToArd

@requires(ProcessRawToArd)
class ConvertToTif(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        # 

#         for f in ./Outputs/*.kea;
#           tasks.append(GdalTranslate(pathRoots=self.PathRoots, inputFile=f))
#        yield to tasks

        # for t in tasks
        #   filePath = t.output().name
        # Create list of expected output files

        with self.output().open('w') as o:
            # write out generated file list
            o.write('some files')

    def output(self):
        # some loigc to determin actual arcsi filelist file name

        outFile = os.path.join(self.pathRoots['state'], 'ConvertToTif.json')
        return LocalTarget(outFile)