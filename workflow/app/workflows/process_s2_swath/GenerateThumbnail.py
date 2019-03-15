import luigi
import os
import subprocess
import process_s2_swath.common as common
from luigi import LocalTarget
from process_s2_swath.CheckFileExists import CheckFileExists

class GenerateThumbnail(luigi.Task):
    # Luigi input variables
    pathRoots = luigi.DictParameter()
    inputFile = luigi.Parameter()

    # Local variables    
    outputFile = ""

    def run(self):
        t = CheckFileExists(self.inputFile)
        yield t
        
        self.outputFile = "{}.jpg".format(os.path.splitext(self.inputFile)[0])

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
