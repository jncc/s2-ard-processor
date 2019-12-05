import json
import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from functional import seq
from .GenerateProductMetadata import GenerateProductMetadata
from .CheckArdProducts import CheckArdProducts

#TODO: Requires CheckArdProducts instead
@requires(CheckArdProducts)
class GenerateMetadata(luigi.Task):
    """
    Output will look like the following;

    {
        "products": [
            {
                "productName": "SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb",
                "files": [
                    "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_meta.xml"
                ]
            },
            {
                "productName": "SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb",
                "files": [
                    "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_meta.xml"
                ]                
            },
            ...
        ]
    }
    """
    paths = luigi.DictParameter()
    metadataTemplate = luigi.Parameter()
    metadataConfigFile = luigi.Parameter()

    def run(self):
        processRawToArdInfo = {}
        with self.input().open("r") as ProcessRawToArd:
            processRawToArdInfo = json.load(ProcessRawToArd)

        metadataConfig = os.path.join(self.paths["static"], self.metadataConfigFile)

        getConfigTask = CheckFileExists(filePath=self.metadataConfigPath)

        yield getConfigTask

        metadataConfig = {}
        with getConfigTask.open('r') as m:
            metadataConfig = json.load(m)

        getTemplateTask = CheckFileExists(filePath=self.metadataTemplatePath)

        metadataTemplate = ""

        with getTemplateTask.open('r') as t:
            metadataTemplate = t.read()

        generateMetadataTasks = []

        # make metadata file/(s) per product?
        for output in processRawToArdInfo["products"]:
            generateMetadataTasks.append(GenerateProductMetadata(pathRoots=self.paths, 
            inputProduct=output, 
            metadataConfig=metadataConfig,
            metadataTemplate=metadataTemplate))

        yield generateMetadataTasks

        products = []
        for task in generateMetadataTasks:
            with task.open('r') as productInfo:
                products.append(json.load(productInfo))
        
        output = {
            "products" : products
        }
            
        with self.output().open('w') as o:
            json.dump(output, o)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'GenerateMetadata.json')
        return LocalTarget(outFile)