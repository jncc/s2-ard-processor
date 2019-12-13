import luigi
import os
import glob
from luigi import LocalTarget

class CheckFileExistsWithPattern(luigi.ExternalTask):
    """
    Checks that a file exists as long as it conforms to a certain pattern 
    provided at initialisation of the task using glob;

    Each pattern MUST only return a single result, otherwise the code will
    raise an exception, this result must exist on the filepath and have some
    content (size > 0)

    The task returns a LocalTarget pointing at the file
    """
    dirPath = luigi.Parameter()
    pattern = luigi.Parameter()

    def output(self):
        # Do a fuzzy check on the filenames because it's too much effort to generate the full expected file names
        
        filePattern = os.path.join(self.dirPath, self.pattern)
        matchingFiles = glob.glob(filePattern)

        if not len(matchingFiles) == 1:
            raise Exception("Something went wrong, found more than one file for pattern " + self.pattern)
        if not os.path.isfile(matchingFiles[0]):
            raise Exception("Something went wrong, " + matchingFiles[0] + " is not a file")
        if not os.path.getsize(matchingFiles[0]) > 0:
            raise Exception("Something went wrong, file size is 0 for " + matchingFiles[0])

        return LocalTarget(matchingFiles[0])