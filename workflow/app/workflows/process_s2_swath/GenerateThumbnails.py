import json
import luigi
import os
import process_s2_swath.common as common
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.OptimiseFiles import OptimiseFiles
from process_s2_swath.GenerateThumbnail import GenerateThumbnail

@requires(OptimiseFiles)
class GenerateThumbnails(luigi.Task):
    """
    Once the input files have been optimised, we need to create one or more 
    thumbnails for the products, typically we only want to create a thumbnail
    for the *_vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.tif file.

    Output should follow the following pattern;

    {
        "products": [
            {
                "productName": "SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb",
                "thumbnails": [
                    "/app/output/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb/SEN2_20190226_lat53lon071_T30UXD_ORB137_utm30n_osgb_thumbnail.jpg"
                ]
            },
            {
                "productName": "SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb",
                "thumbnails": [
                    "/app/output/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb/SEN2_20190226_lat52lon089_T31UCT_ORB137_utm31n_osgb_thumbnail.jpg"
                ]                
            },
            ...
        ]
    }    
    """
    pathRoots = luigi.DictParameter()

    def run(self):
        with self.input().open('r') as optimiseFilesFile:
            optimiseFilesJson = json.loads(optimiseFilesFile)

            tasks = []
            # TODO: make list of files to create thumbnails for vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.tif ...
            # Only interested in one file right now
            for filename in filter(lambda x: x.endswith("vmsk_sharp_mclds_topshad_rad_srefdem_stdsref.tif"), list(optimiseFilesJson["optimisedFiles"])):
                tasks.append(GenerateThumbnail(pathRoots=self.pathRoots, inputFile=filename))
            
            # Make thumbnail for images 
            yield tasks

            generatedThumbnails = []

            # TODO: check thumbnail(s) have been created
            for task in tasks:
                generatedThumbnails.append(task.output().fn)

        with self.output().open('w') as o:
            o.write(common.getFormattedJson({
                "generatedThumbnails": generatedThumbnails
            }))

    def output(self):
        outFile = os.path.join(self.pathRoots["state"], "GenerateThumbnails.json")
        return LocalTarget(outFile)