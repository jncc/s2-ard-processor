import json
import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from functional import seq
from process_s2_swath.GenerateProductMetadata import GenerateProductMetadata
from process_s2_swath.CheckArdProducts import CheckArdProducts
from process_s2_swath.GetSwathInfo import GetSwathInfo
from process_s2_swath.GetArcsiMetadata import GetArcsiMetadata
from process_s2_swath.RenameOutputs import RenameOutputs
from process_s2_swath.CheckFileExists import CheckFileExists

@requires(CheckArdProducts, RenameOutputs, GetSwathInfo, GetArcsiMetadata)
class GenerateMetadata(luigi.Task):
    """
    Output will look like the following;

    {
        "products": [
            {
                "productName": "S2A_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb",
                "files": [
                    "/app/output/S2A_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/S2A_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_meta.xml"
                ]
            },
            {
                "productName": "S2A_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb",
                "files": [
                    "/app/output/S2A_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/S2A_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_meta.xml"
                ]                
            },
            ...
        ]
    }
    """
    paths = luigi.DictParameter()
    metadataTemplate = luigi.Parameter()
    metadataConfigFile = luigi.Parameter()
    buildConfigFile = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        metadataConfigPath = os.path.join(self.paths["static"], self.metadataConfigFile)

        getConfigTasks = []
        getConfigTasks.append(CheckFileExists(filePath=metadataConfigPath))
        getConfigTasks.append(CheckFileExists(filePath=self.buildConfigFile))

        yield getConfigTasks

        metadataConfig = {}
        with getConfigTasks[0].output().open('r') as m:
            metadataConfig = json.load(m)

        buildConfig = {}
        with getConfigTasks[1].output().open('r') as b:
            buildConfig = json.load(b)

        getTemplateTask = CheckFileExists(filePath=self.metadataTemplate)

        yield getTemplateTask

        ardProducts = {}
        with self.input()[0].open("r") as CheckArdProductsFile:
            ardProducts = json.load(CheckArdProductsFile)

        renameOutputs = {}
        with self.input()[1].open("r") as RenameOutputsFile:
            renameOutputs = json.load(RenameOutputsFile)

        getSwathInfo = {}
        with self.input()[2].open("r") as GetSwatchInfoFile:
            getSwathInfo = json.load(GetSwatchInfoFile)

        getArcsiMetadata = {}
        with self.input()[3].open("r") as GetArcsiMetadataFile:
            getArcsiMetadata = json.load(GetArcsiMetadataFile)

        generateMetadataTasks = []

        # make metadata file/(s) per product?
        for product in ardProducts["products"]:
            renamedOutput = seq(renameOutputs["products"]) \
                .where(lambda x: x["productName"] == product["productName"]) \
                .first()

            ardProductName = renamedOutput["ardProductName"]

            granuleInfo = seq(getSwathInfo["products"]) \
                .where(lambda x: x["productName"] == product["productName"]) \
                .first()

            arcsiInfo = seq(getArcsiMetadata["products"]) \
                .where(lambda x: x["productName"] == product["productName"]) \
                .first()
        
            generateMetadataTasks.append(GenerateProductMetadata(paths=self.paths, 
                inputProduct=product,
                metadataConfig=metadataConfig,
                buildConfig=buildConfig,
                metadataTemplate=self.metadataTemplate,
                outputDir = ardProducts["outputDir"],
                ardProductName = ardProductName,
                granuleInfo = granuleInfo,
                arcsiInfo = arcsiInfo,
                testProcessing = self.testProcessing)
            )

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