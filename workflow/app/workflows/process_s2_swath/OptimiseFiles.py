import json
import luigi
import os
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.BuildPyramidsAndCalulateStats import BuildPyramidsAndCalulateStats
from process_s2_swath.CalculateStats import CalculateStats
from process_s2_swath.ConvertToTif import ConvertToTif

@requires(ConvertToTif)
class OptimiseFiles(luigi.Task):
    """
    After the KEA files have been converted to GeoTIFF files then we need to reapply some
    file optimisations (build image pyramids and calculate statistics), this is done with
    an rsgislib helper function in the BuildPyramidsAndCalculateStats task, the outputs here,
    look like;

    {
        "products": [
            {
                "productName": "SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb",
                "optimisedFiles": [
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
                "optimisedFiles": [
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

        with self.input().open("r") as convertToTifFile:
            convertToTifJson = json.load(convertToTifFile)

            optimiseTasks = []

            for filename in convertToTifJson["convertedFiles"]:
                optimiseTasks.append(BuildPyramidsAndCalulateStats(pathRoots=self.pathRoots, inputFile=filename))

            yield optimiseTasks

        with self.output().open('w') as o:
            convertToTifJson["builtPyramids"] = True
            convertToTifJson["calculatedStats"] = True
            json.dump(convertToTifJson)

    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'OptimiseFiles.json')
        return LocalTarget(outFile)