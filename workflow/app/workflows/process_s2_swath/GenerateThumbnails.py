import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.ConvertToTif import ConvertToTif

@requires(ConvertToTif)
class GenerateThumbnails(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        # TODO: make list of files to create thumbnails for

        # TODO: make thumbnail for images

        # TODO: check thumbnail(s) have been created

        with self.output().open('w') as o:
            # write out processed file list
            o.write('some files')

    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'GenerateThumbnails.json')
        return LocalTarget(outFile)