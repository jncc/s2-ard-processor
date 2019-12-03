import json
import logging
import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from functional import seq
from .common import getFormattedJson
from .CreateCOG import CreateCOG
from .CheckArdProducts import CheckArdProducts

log = logging.getLogger('luigi-interface')

@requires(CheckArdProducts)
class CreateCOGs(luigi.Task):
    """
    Converts all KEA files into GeoTIFF's, extracts a list of files to convert,
    and then create a task for each of those files, the scheduler then decides
    when to run the conversions (upto a limit of workers in this case)

    Outputs for this will be similar to the previous ProcessToArd outputs;

    {
        "products": [
            {
                "productName": "SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb",
                "files": [
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
                "files": [
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
    maxCogProcesses = luigi.IntParameter(default=4)

    def run(self):

        processRawToArdInfo = {}

        with self.input().open('r') as processRawToArdFile:
            processRawToArdInfo = json.load(processRawToArdFile)
    
        # filesToConvert = list(filter(lambda x: os.path.splitext(x)[1] == '.kea', processRawToArdInfo['files']))

        cogTasks = []
        for p in processRawToArdInfo["products"]:
            cogTasks.append(CreateCOG(pathRoots=self.pathRoots, product=p, maxCogProcesses=self.maxCogProcesses))
        
        yield cogTasks
        
        cogProducts = []
        for task in cogTasks:
            with task.open('r') as cogInfo:
                cogProducts.append(json.load(cogInfo))

        numFilesToConvert = seq(processRawToArdInfo["products"]) \
                        .map(lambda x: x["files"]) \
                        .flatten() \
                        .count(lambda x: os.path.splitext(x)[1] == '.kea')

        numCogProducts = seq(processRawToArdInfo["products"]) \
                        .map(lambda x: x["files"]) \
                        .flatten() \
                        .count()

        if not numFilesToConvert == numCogProducts:
            log.error("""The length of known files to convert to tif is not the same as the number of converted files, expected conversions for the files;
                Expected:
                {}
                Found:
                {}
                Missing:
                {}""".format(numFilesToConvert, 
                    numCogProducts, 
                    (numFilesToConvert - numCogProducts)
                )
            )
            raise RuntimeError("Not all files were converted from kea to tif files")

        output = {"products": cogProducts}

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'CreateCOGs.json')
        return LocalTarget(outFile)