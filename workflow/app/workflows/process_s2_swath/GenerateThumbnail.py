import luigi
import os
import subprocess
import process_s2_swath.common as common
from luigi import LocalTarget
from process_s2_swath.CheckFileExists import CheckFileExists

class GenerateThumbnail(luigi.Task):
    """
    Creates a thumbnail using the provided input file using gdal_translate
    to create a jpg version of the input file at the specified output file 
    path (image will use bands 3,2,1 and output at 5% of original image size)

    Outputs a LocalTarget pointing at the created output file
    """
    # Luigi input variables
    inputFile = luigi.Parameter()
    outputFile = luigi.Parameter()

    def run(self):
        t = CheckFileExists(self.inputFile)
        yield t
        
        #'gdal_translate -b 3 -b 2 -b 1 -ot Byte -of JPEG -outsize 5%% 5%% %s %s' % (input_path, output_path)
        cmd = "gdal_translate -of JPEG -ot Byte -outsize 5%% 5%% -b 3 -b 2 -b 1 {} {}".format(self.inputFile, self.outputFile)
        command_line_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True)
        
        # TODO: logging probably doesn't work
        process_output, _ =  command_line_process.communicate()
        log.info(process_output)

        t = CheckFileExists(self.outputFile)
        yield t

    def output(self):
        return LocalTarget(self.outputFile)
