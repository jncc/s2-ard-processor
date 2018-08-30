import luigi
import os
import json
import subprocess
import logging
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.BuildFileList import BuildFileList

log = logging.getLogger('luigi-interface')

@requires(BuildFileList)
class RunArcsi(luigi.Task):
    pathRoots = luigi.DictParameter()
    dem = luigi.Parameter()

    def run(self):
        buildFileListOutput = {}
        with self.input().open('r') as i:
            buildFileListOutput = json.loads(i.read())

        demFilePath = os.path.join(self.pathRoots["static"], self.dem)
        fileListPath = buildFileListOutput["fileListPath"]

        cmd = "arcsi.py -s sen2 --stats -f KEA --fullimgouts -p RAD SHARP SATURATE CLOUDS TOPOSHADOW STDSREF DOSAOTSGL METADATA \
            --interpresamp near --interp cubic -t {} -o {} --projabbv osgb --outwkt /app/BritishNationalGrid.wkt --dem {} \
            -k clouds.kea meta.json sat.kea toposhad.kea valid.kea stdsref.kea --multi -i {}" \
            .format(
                self.pathRoots["temp"],
                self.pathRoots["temp"],
                demFilePath,
                fileListPath
            )

        try:
            log.info("Running cmd: " + cmd)
            subprocess.check_output(cmd, shell=True) 
        except subprocess.CalledProcessError as e:
            errStr = "command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output)
            log.error(errStr)
            raise RuntimeError(errStr)

        # Create list of expected output files

        with self.output().open('w') as o:
            o.write('some files')

    def output(self):
        # some loigc to determin actual arcsi filelist file name

        outFile = os.path.join(self.pathRoots['state'], 'RunArcsi.json')
        return LocalTarget(outFile)
