import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.BuildFileList import BuildFileList

@requires(BuildFileList)
class RunArcsi(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        # arcsi.py -s sen2 --stats -f KEA --fullimgouts 
        # -p RAD SHARP SATURATE CLOUDS TOPOSHADOW STDSREF DOSAOTSGL METADATA 
        # --interpresamp near 
        # --interp cubic 
        # -t /data/tmp/ 
        # -o /data/Outputs/ 
        # --projabbv osgb 
        # --outwkt /data/BritishNationalGrid.wkt 
        # --dem /data/dem/DTM_UK_10m_OSGB_CompImg.kea 
        # -k clouds.kea meta.json sat.kea toposhad.kea valid.kea stdsref.kea 
        # --multi
        # -i /data/Outputs/$LIST_FILENAME

        # Create list of expected output files

        with self.output().open('w') as o:
            o.write('some files')

    def output(self):
        # some loigc to determin actual arcsi filelist file name

        outFile = os.path.join(self.pathRoots['state'], 'RunArcsi.json')
        return LocalTarget(outFile)
