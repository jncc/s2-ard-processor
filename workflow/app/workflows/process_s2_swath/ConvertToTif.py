import json
import logging
import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.common import getFormattedJson
from process_s2_swath.ProcessRawToArd import ProcessRawToArd
from process_s2_swath.GdalTranslateKeaToTif import GdalTranslateKeaToTif

log = logging.getLogger('luigi-interface')

@requires(ProcessRawToArd)
class ConvertToTif(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):

        with self.input().open('r') as processRawToArdFile:
            processRawToArdJson = json.load(processRawToArdFile)
            filesToConvert = list(filter(lambda x: os.path.splitext(x)[1] == 'kea', processRawToArdJson['files']))

            convertTasks = []
            for filename in filesToConvert:
                convertTasks.append(GdalTranslateKeaToTif(pathRoots=self.pathRoots, inputFile=filename))
            
            yield convertTasks
            
            convertedFileOutput = []
            for task in convertTasks:
                convertedFileOutput.append(task.output().fn)

            if not len(filesToConvert) == len(convertedFileOutput):
                log.error("""The length of known files to convert to tif is no the same as the number of converted files, expected conversions for the files;
                    {}
                    Found:
                    {}
                    Missing:
                    {}""".format(filesToConvert, 
                        convertedFileOutput, 
                        set(map(lambda x: os.path.splitext(x)[0], filesToConvert)).difference(set(map(lambda x: os.path.splitext(x)[0], convertedFileOutput)))
                    )
                )
                raise RuntimeError("Not all files were converted from kea to tif files")

        with self.output().open('w') as o:
            o.write(getFormattedJson(json.dumps({
                "generatedThumbnails": convertedFileOutput
            })))

    def output(self):
        # some loigc to determin actual arcsi filelist file name

        outFile = os.path.join(self.pathRoots['state'], 'ConvertToTif.json')
        return LocalTarget(outFile)