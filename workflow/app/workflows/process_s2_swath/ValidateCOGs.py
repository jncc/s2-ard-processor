import json
import logging
import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from functional import seq
from process_s2_swath.CreateCOGs import CreateCOGs
from process_s2_swath.ValidateCOG import ValidateCOG

log = logging.getLogger('luigi-interface')

@requires(CreateCOGs)
class ValidateCOGs(luigi.Task):
    """
    Run validate_cloud_optimized_geotiff.py on each of the geotiffs

    Outputs for this will look like;

    {
        "products": [
            {
                "productName": "SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb",
                "files": [
                    {
                        "file": "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_clouds.tif",
                        "validCog": true
                    },
                    {
                        "file": "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_sat.tif",
                        "validCog": true
                    },
                    {
                        "file": "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_toposhad.tif",
                        "validCog": true
                    },
                    {
                        "file": "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_valid.tif",
                        "validCog": true
                    },
                    {
                        "file": "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.tif",
                        "validCog": true
                    },
                    {
                        "file": "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_vmsk_sharp_rad_srefdem_stdsref.tif",
                        "validCog": true
                    }               
                ]
            },
            {
                "productName": "SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb",
                "files": [
                    {
                        "file": "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_clouds.tif",
                        "validCog": true
                    },
                    {
                        "file": "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_sat.tif",
                        "validCog": true
                    },
                    {
                        "file": "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_toposhad.tif",
                        "validCog": true
                    },
                    {
                        "file": "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_valid.tif",
                        "validCog": true
                    },
                    {
                        "file": "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.tif",
                        "validCog": true
                    },
                    {
                        "file": "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_vmsk_sharp_rad_srefdem_stdsref.tif"
                        "validCog": true
                    }
                ]                
            },
            ...
        ]
    }
    """
    paths = luigi.DictParameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):

        products = {}
        with self.input().open('r') as CreateCOGsFile:
            products = json.load(CreateCOGsFile)

        validateTasks = []
        for p in products["products"]:
            validateTasks.append(ValidateCOG(paths=self.paths, 
                product=p, 
                maxCogProcesses=self.maxCogProcesses,
                testProcessing=self.testProcessing))
        
        yield validateTasks
        
        validCogProducts = []
        for task in validateTasks:
            with task.output().open('r') as cogInfo:
                validCogProducts.append(json.load(cogInfo))

        output = {
            "outputDir": products["outputDir"],
            "products": validCogProducts
        }

        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'ValidateCOGs.json')
        return LocalTarget(outFile)