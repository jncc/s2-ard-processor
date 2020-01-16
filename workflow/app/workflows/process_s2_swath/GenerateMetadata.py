import json
import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from functional import seq
from process_s2_swath.GenerateProductMetadata import GenerateProductMetadata
from process_s2_swath.CheckArdProducts import CheckArdProducts
from process_s2_swath.CheckFileExists import CheckFileExists

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
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        metadataConfigPath = os.path.join(self.paths["static"], self.metadataConfigFile)

        getConfigTask = CheckFileExists(filePath=metadataConfigPath)

        yield getConfigTask

        metadataConfig = {}
        with getConfigTask.output().open('r') as m:
            metadataConfig = json.load(m)

        getTemplateTask = CheckFileExists(filePath=self.metadataTemplate)

        yield getTemplateTask

        ardProducts = {}
        with self.input().open("r") as CheckArdProductsFile:
            ardProducts = json.load(CheckArdProductsFile)

        generateMetadataTasks = []

        # make metadata file/(s) per product?
        for product in ardProducts["products"]:
            generateMetadataTasks.append(GenerateProductMetadata(paths=self.paths, 
            inputProduct=product, 
            metadataConfig=metadataConfig,
            metadataTemplate=self.metadataTemplate,
            outputDir = ardProducts["outputDir"],
            testProcessing = self.testProcessing))

        yield generateMetadataTasks

        products = []
        for task in generateMetadataTasks:
            with task.output().open('r') as productInfo:
                products.append(json.load(productInfo))
        
        output = {
            "outputDir" : ardProducts["outputDir"],
            "products" : products
        }
            
        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'GenerateMetadata.json')
        return LocalTarget(outFile)