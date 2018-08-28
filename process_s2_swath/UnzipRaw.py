import luigi
import os
from luigi import LocalTarget
from luigi.util import requires

class UnzipRaw(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        # runs arcsi extract

        with self.output().open('w') as o:
            o.write('UnzipRaw')
    
    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'UnzipRaw.json')
        return LocalTarget(outFile)
        
@requires(UnzipRaw)
class BuildFileList(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        with self.output().open('w') as o:
            o.write('some files')

    def output(self):
        # some loigc to determin actual arcsi filelist file name

        outFile = os.path.join(self.pathRoots['state'], 'fileList.txt')
        return LocalTarget(outFile)

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

class CheckFileExists(luigi.ExternalTask):
    filePath = luigi.Parameter()

    def output(self):
        #check file size

        return LocalTarget(self.filePath)

class GdalTranslate(luigi.Task):
    pathRoots = luigi.DictParameter()
    inputFile = luigi.Parameter()

    def run(self):
        t = CheckFileExists(self.inputFile)
        yield t
        #           gdal_translate -of GTiff -co "COMPRESS=LZW" -co "TILED=YES" -co "BLOCKXSIZE=256" -co "BLOCKYSIZE=256" -co "BIGTIFF=YES" $f ${f%.*}.tif;
        # generate output file name /file/path/name.tif

    def output(self):
        # outFile =output path/ generated output file name
        return LocalTarget(outFile)

@requires(RunArcsi)
class ConvertToTif(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        # 

#         for f in ./Outputs/*.kea;
#           tasks.append(GdalTranslate(pathRoots=self.PathRoots, inputFile=f))
#        yield to tasks

        # for t in tasks
        #   filePath = t.output().name
        # Create list of expected output files

        with self.output().open('w') as o:
            # write out generated file list
            o.write('some files')

    def output(self):
        # some loigc to determin actual arcsi filelist file name

        outFile = os.path.join(self.pathRoots['state'], 'CheckOutputFilesExist.json')
        return LocalTarget(outFile)

class BuildPyramid(luigi.Task):
    pathRoots = luigi.DictParameter()
    inputFile = luigi.Parameter()

    def run(self):
        t = CheckFileExists(self.inputFile)
        yield t
        # import os.path
        # import sys
        # import rsgislib
        # from rsgislib import imageutils

        # rsgislib.imageutils.popImageStats(os.path.join(inDir, file), True, 0., True)
        # generate output file name /file/path/name.tif

    def output(self):
        # outFile =output path/ generated output file name
        return LocalTarget(outFile)

@requires(ConvertToTif)
class BuildPyramids(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):

# inDir = 'Outputs'

# files = [file for file in os.listdir(inDir) if file.endswith('.tif')]

# for file in files:   


#         for f in ./Outputs/*.kea;
#           tasks.append(BuildPyramid(pathRoots=self.PathRoots, inputFile=f))
#        yield to tasks

        # for t in tasks
        #   filePath = t.output().name
        # Create list of expected output files

        with self.output().open('w') as o:
            # write out generated file list
            o.write('some files')

    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'BuildPyramids.json')
        return LocalTarget(outFile)

@requires(BuildPyramids)
class CheckOutputFilesExist(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        # with self.input
        #  for each file
        #    create task to check file
        #  yield to tasks

        with self.output().open('w') as o:
            # write out input file list
            o.write('some files')

    def output(self):
        # some loigc to determin actual arcsi filelist file name

        outFile = os.path.join(self.pathRoots['state'], 'CheckOutputFilesExist.json')
        return LocalTarget(outFile)

@requires(CheckOutputFilesExist)
class GenerateMetadata(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        # make metadata file

        with self.output().open('w') as o:
            # write out input file list
            o.write('some files')

    def output(self):
        # some loigc to determin actual arcsi filelist file name

        outFile = os.path.join(self.pathRoots['state'], 'metadata.xml')
        return LocalTarget(outFile)
