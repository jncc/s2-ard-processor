import luigi
import os
import glob
from luigi import LocalTarget

class CheckFileExistsWithPattern(luigi.ExternalTask):
    dirPath = luigi.Parameter()
    pattern = luigi.Parameter()

    def output(self):
        # Do a fuzzy check on the filenames because it's too much effort to generate the full expected file names
        
        filePattern = os.path.join(self.dirPath, self.pattern)
        matchingFiles = glob.glob(filePattern) # haven't tested this yet

        if not len(matchingFiles) == 1:
            raise Exception("Something went wrong, found more than one file for pattern " + self.pattern)
        if not os.path.isfile(matchingFiles[0]):
            raise Exception("Something went wrong, " + matchingFiles[0] + " is not a file")
        if not os.path.getsize(matchingFiles[0]) > 0:
            raise Exception("Something went wrong, file size is 0 for " + matchingFiles[0])
        return LocalTarget(matchingFiles[0])