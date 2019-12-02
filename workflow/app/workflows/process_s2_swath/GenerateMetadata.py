import json
import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from functional import seq
from .CheckFileExistsWithPattern import CheckFileExists
from .GenerateProductMetadata import GenerateProductMetadata
from .ProcessRawToArd import ProcessRawToArd

@requires(ProcessRawToArd)
class GenerateMetadata(luigi.Task):
    """
    When we have created the final outputs we need to generate a set of 
    metadata files for forward consumption, TODO: define these files!

    TODO: Currently only pushing out the metadata file created by ARCSI, but
    need to create GEMINI and CEOS metdata files here;

    Output will look like the following;

    {
        "products": [
            {
                "productName": "SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb",
                "metadata": [
                    "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_meta.json",
                    "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_meta_gemini.xml",
                    "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_meta_ceos.xml"
                ]
            },
            {
                "productName": "SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb",
                "metadata": [
                    "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_meta.json",
                    "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_meta_gemini.xml",
                    "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_meta_ceos.xml"
                ]                
            },
            ...
        ]
    }
    """
    pathRoots = luigi.DictParameter()
    metadataTemplatePath = luigi.Parameter()
    metadataConfigPath = luigi.Parameter()

    def run(self):
        processRawToArdInfo = {}
        with self.input().open("r") as ProcessRawToArd:
            processRawToArdInfo = json.load(ProcessRawToArd)

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
            generateMetadataTasks.append(GenerateProductMetadata(pathRoots=self.pathRoots, 
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
        outFile = os.path.join(self.pathRoots['state'], 'GenerateMetadata.json')
        return LocalTarget(outFile)