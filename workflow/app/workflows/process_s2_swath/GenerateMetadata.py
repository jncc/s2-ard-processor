import json
import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.CheckFileExists import CheckFileExists
from process_s2_swath.GenerateProductMetadata import GenerateProductMetadata
from process_s2_swath.ProcessRawToArd import ProcessRawToArd

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

    def run(self):
        with self.input().open("r") as outputFile:
            outputFileJson = json.read(outputFile)

            generateMetadataTasks = []

            # make metadata file/(s) per product?
            for output in outputFileJson["convertedFiles"]:
                # TODO: Generate metadata task?
                generateMetadataTasks.append(GenerateProductMetadata(pathRoots=self.pathRoots, inputProduct=output))

            yield generateMetadataTasks

            for task in generateMetadataTasks:
                # TODO: do something?

        with self.output().open('w') as o:
            outputFileJson["generatedMetadata"] = True # TODO: per product more fine grained handling?
            json.dump(outputFileJson, o)

    def output(self):
        # some loigc to determin actual arcsi filelist file name
        outFile = os.path.join(self.pathRoots['state'], 'generatedMetadata.json')
        return LocalTarget(outFile)