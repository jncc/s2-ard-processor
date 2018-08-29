import luigi
import os
import shutil
from luigi import LocalTarget
from luigi.util import requires

class PrepareWorkingFolder(luigi.Task):
    pathRoots = luigi.DictParameter()

    def makePath(self, newPath):
        if not os.path.exists(newPath):
            os.makedirs(newPath)

    def clearFolder(self, folder):
        for f in os.listdir(folder):
            path = os.path.join(folder, f)

            if os.path.isfile(path):
                os.unlink(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)

    def run(self):
        if (os.path.exists(self.pathRoots['extracted'])):
            self.clearFolder(self.pathRoots['extracted'])

        if (os.path.exists(self.pathRoots['temp'])):
            self.clearFolder(self.pathRoots['temp'])

        self.makePath(self.pathRoots['extracted'])
        self.makePath(self.pathRoots['temp'])
    
        with self.output().open('w') as o:
            o.write('\{\}')

    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'PrepareWorkingFolder_SUCCESS.json')
        return LocalTarget(outFile)
