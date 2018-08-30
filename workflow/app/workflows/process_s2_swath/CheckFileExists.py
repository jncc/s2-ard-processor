import luigi
import os
from luigi import LocalTarget

class CheckFileExists(luigi.ExternalTask):
    filePath = luigi.Parameter()

    def output(self):
        if not os.path.getsize(self.filePath) > 0:
            raise Exception("Something went wrong, file size is 0 for " + self.filePath)
        return LocalTarget(self.filePath)