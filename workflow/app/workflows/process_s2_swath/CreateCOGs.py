import json
import logging
import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from functional import seq
from process_s2_swath.CreateCOG import CreateCOG
from process_s2_swath.ValidateCOG import ValidateCOG
from process_s2_swath.CheckArdProducts import CheckArdProducts

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
                    "/working/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_clouds.tif",
                    "/working/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_sat.tif",
                    "/working/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_toposhad.tif",
                    "/working/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_valid.tif",
                    "/working/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.tif",
                    "/workingp/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_vmsk_sharp_rad_srefdem_stdsref.tif"
                ]
            },
            {
                "productName": "SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb",
                "files": [
                    "/working/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_clouds.tif",
                    "/working/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_sat.tif",
                    "/working/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_toposhad.tif",
                    "/working/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_valid.tif",
                    "/working/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.tif",
                    "/working/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_vmsk_sharp_rad_srefdem_stdsref.tif"
                ]
            },
            ...
        ]
    }
    """
    paths = luigi.DictParameter()
    maxCogProcesses = luigi.IntParameter(default=4)
    validateCogs = luigi.BoolParameter(default = False)
    validateCogScriptDir = luigi.Parameter(default = "/app")
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):

        ardProducts = {}

        with self.input().open('r') as CheckArdProductsFile:
            ardProducts = json.load(CheckArdProductsFile)
    
        # filesToConvert = list(filter(lambda x: os.path.splitext(x)[1] == '.kea', processRawToArdInfo['files']))

        cogTasks = []
        for p in ardProducts["products"]:
            cogTasks.append(CreateCOG(paths=self.paths, 
                product=p, 
                maxCogProcesses=self.maxCogProcesses,
                testProcessing=self.testProcessing))
        
        yield cogTasks
        
        cogProducts = []
        for task in cogTasks:
            with task.output().open('r') as cogInfo:
                cogProducts.append(json.load(cogInfo))

        numFilesToConvert = seq(ardProducts["products"]) \
                        .map(lambda x: x["files"]) \
                        .flatten() \
                        .count(lambda x: os.path.splitext(x)[1] == '.kea')

        numCogProducts = seq(cogProducts) \
                        .map(lambda x: x["files"]) \
                        .flatten() \
                        .count(lambda x: os.path.splitext(x)[1] == '.tif')

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

        if self.validateCogs:
            validateCogTasks = []
            for p in cogProducts:
                validateCogTasks.append(ValidateCOG(paths=self.paths, 
                    product=p, 
                    maxCogProcesses=self.maxCogProcesses,
                    validateCogScriptDir=self.validateCogScriptDir,
                    testProcessing=self.testProcessing))
            
            yield validateCogTasks

        output = {
            "outputDir": ardProducts["outputDir"],
            "products": cogProducts
            }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'CreateCOGs.json')
        return LocalTarget(outFile)