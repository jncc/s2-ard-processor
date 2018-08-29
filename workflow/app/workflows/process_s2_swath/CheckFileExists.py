import luigi
from luigi import LocalTarget

class CheckFileExists(luigi.ExternalTask):
    filePath = luigi.Parameter()

    def output(self):
        #check file size

        return LocalTarget(self.filePath)