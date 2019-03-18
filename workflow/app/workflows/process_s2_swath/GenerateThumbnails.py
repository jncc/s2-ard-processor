import json
import luigi
import os
import process_s2_swath.common as common
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.CheckOutputFilesExist import CheckOutputFilesExist
from process_s2_swath.GenerateThumbnail import GenerateThumbnail

@requires(CheckOutputFilesExist)
class GenerateThumbnails(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        with self.input().open('r') as convertToTifFile:
            convertToTifJson = json.loads(convertToTifFile)

            tasks = []
            # TODO: make list of files to create thumbnails for vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.tif ...
            # Only interested in one file right now
            for filename in filter(lambda x: x.endswith("vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.tif"), list(convertToTifJson["convertedFiles"])):
                tasks.append(GenerateThumbnail(pathRoots=self.pathRoots, inputFile=filename))
            
            # Make thumbnail for images 
            yield tasks

            generatedThumbnails = []

            # TODO: check thumbnail(s) have been created
            for task in tasks:
                generatedThumbnails.append(task.output().fn)

        with self.output().open('w') as o:
            o.write(common.getFormattedJson({
                "generatedThumbnails": generatedThumbnails
            }))

    def output(self):
        outFile = os.path.join(self.pathRoots["state"], "GenerateThumbnails.json")
        return LocalTarget(outFile)