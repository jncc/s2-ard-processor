import json
import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.ProcessRawToArd import ProcessRawToArd

@requires(ProcessRawToArd)
class CheckOutputFilesExist(luigi.Task):
    """
    Runs after the process raw to ard task? 
    TODO: Is this actually needed, we check expected file patterns in that
    step already

    outputs previous step input with outputsExist = True flag?
    """
    pathRoots = luigi.DictParameter()

    def run(self):
        # with self.input
        #  for each file
        #    create task to check file
        #  yield to tasks
        with self.input().open("r") as processedRawToArdFile:
            processedRawToArdJson = json.read(processedRawToArdFile)

            checkFileTasks = []

            for outputFilename in processedRawToArdJson["outputFiles"]:
                checkFileTasks.append(CheckFileExists(inputFile=outputFilename))

            yield checkFileTasks

            for task in checkFileTasks:
                # TODO: do something?

        with self.output().open('w') as o:
            # write out input file list
            optimisedFilesJson["outputsExist"] = True
            json.dump(optimisedFilesJson, o)

    def output(self):
        # some loigc to determin actual arcsi filelist file name
        outFile = os.path.join(self.pathRoots['state'], 'CheckOutputFilesExist.json')
        return LocalTarget(outFile)