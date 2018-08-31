import luigi
import os
import json
import subprocess
import logging
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.BuildFileList import BuildFileList

log = logging.getLogger('luigi-interface')

# require ReadManifests to get the metadata bits for the output filenames
@requires(BuildFileList)
class RunArcsi(luigi.Task):
    pathRoots = luigi.DictParameter()
    dem = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)

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

        if not self.testProcessing:
            try:
                log.info("Running cmd: " + cmd)
                subprocess.check_output(cmd, shell=True) 
            except subprocess.CalledProcessError as e:
                errStr = "command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output)
                log.error(errStr)
                raise RuntimeError(errStr)
        else:
            log.info("Generating mock output files")
            # make test files, see examples: http://gws-access.ceda.ac.uk/public/defra_eo/sentinel/2/processed/ard/NaturalEngland/
            # for each input granule I think we're expecting a file ending in clouds.tif, meta.json, sat.tif, toposhad.tif, valid.tif, vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.tif, and vmsk_sharp_rad_srefdem_stdsref.tif

        # CheckFileExistsWithPattern for all outputs

        with self.output().open('w') as o:
            o.write('some files')

    def output(self):
        # some loigc to determin actual arcsi filelist file name

        outFile = os.path.join(self.pathRoots['state'], 'RunArcsi.json')
        return LocalTarget(outFile)
