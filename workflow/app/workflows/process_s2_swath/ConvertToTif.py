import json
import logging
import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.common import getFormattedJson
from process_s2_swath.GdalTranslateKeaToTif import GdalTranslateKeaToTif
from process_s2_swath.ProcessRawToArd import ProcessRawToArd

log = logging.getLogger('luigi-interface')

@requires(ProcessRawToArd)
class ConvertFilesToTif(luigi.Task):
    """
    Converts all KEA files into GeoTIFF's, extracts a list of files to convert,
    and then create a task for each of those files, the scheduler then decides
    when to run the conversions (upto a limit of workers in this case)

    Outputs for this will be similar to the previous ProcessToArd outputs;

    {
        "products": [
            {
                "productName": "SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb",
                "convertedFiles": [
                    "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_clouds.tif",
                    "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_sat.tif",
                    "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_toposhad.tif",
                    "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_valid.tif",
                    "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.tif",
                    "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_vmsk_sharp_rad_srefdem_stdsref.tif"                    
                ]
            },
            {
                "productName": "SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb",
                "convertedFiles": [
                    "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_clouds.tif",
                    "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_sat.tif",
                    "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_toposhad.tif",
                    "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_valid.tif",
                    "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.tif",
                    "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_vmsk_sharp_rad_srefdem_stdsref.tif"
                ]                
            },
            ...
        ]
    }
    """
    pathRoots = luigi.DictParameter()

    def run(self):

        with self.input().open('r') as checkOutputFilesExistFile:
            checkOutputFilesExistJson = json.load(checkOutputFilesExistFile)
            # TODO: This doesn't actually do what its supposed to do anymore pending with changes from previous ProcessToArd step, but we
            # will need to work with that structure i.e.;
            # for each product -> extract list of kea files (minus json metadata file) -> convert to tif -> store in final output directory
            filesToConvert = list(filter(lambda x: os.path.splitext(x)[1] == 'kea', checkOutputFilesExistJson['files']))

            convertTasks = []
            for filename in filesToConvert:
                convertTasks.append(GdalTranslateKeaToTif(pathRoots=self.pathRoots, inputFile=filename))
            
            yield convertTasks
            
            convertedFileOutput = []
            for task in convertTasks:
                convertedFileOutput.append(task.output().fn)

            if not len(filesToConvert) == len(convertedFileOutput):
                log.error("""The length of known files to convert to tif is no the same as the number of converted files, expected conversions for the files;
                    {}
                    Found:
                    {}
                    Missing:
                    {}""".format(filesToConvert, 
                        convertedFileOutput, 
                        set(map(lambda x: os.path.splitext(x)[0], filesToConvert)).difference(set(map(lambda x: os.path.splitext(x)[0], convertedFileOutput)))
                    )
                )
                raise RuntimeError("Not all files were converted from kea to tif files")

        with self.output().open('w') as o:
            o.write(getFormattedJson(json.dumps({
                "convertedFiles": convertedFileOutput
            })))

    def output(self):
        # some loigc to determin actual arcsi filelist file name

        outFile = os.path.join(self.pathRoots['state'], 'ConvertToTif.json')
        return LocalTarget(outFile)