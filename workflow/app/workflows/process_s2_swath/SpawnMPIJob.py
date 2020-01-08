import luigi
import json
import logging
import os
import subprocess

from string import Template
from luigi import LocalTarget
from functional import seq
from process_s2_swath.CheckFileExists import CheckFileExists

log = logging.getLogger("luigi-interface")

class SpawnMPIJob(luigi.Task):
    paths = luigi.DictParameter()
    dem = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)
    outWkt = luigi.OptionalParameter()
    projAbbv = luigi.OptionalParameter()
    jasminMpiConfig = luigi.Parameter()
    productCount = luigi.IntParameter()
    tempOutDir = luigi.Parameter()
    fileListPath = luigi.Parameter()


    def run(self):
        # load configuration
        mpiConfigPath = os.path.join(self.paths["working"], self.jasminMpiConfig)
        getConfigTask = CheckFileExists(filePath=mpiConfigPath)

        yield getConfigTask

        mpiConfig = {}
        with getConfigTask.output().open('r') as m:
            mpiConfig = json.load(m)

        #load template
        bsubTemplateLocation = os.path.join(self.paths["static"], mpiConfig["jobTemplate"])

        getTemplateTask = CheckFileExists(filePath=bsubTemplateLocation)

        yield getTemplateTask

        with getConfigTask.output().open('r') as t:
            template = Template(t.read())

        # compute nodes
        nodes = self.productCount + 1
        mounts = seq(mpiConfig["container"]["mounts"]) \
            .map(lambda x: "--bind {}:{}".format(x[0], x[1])) \
            .reduce(lambda x, y: "{} {}".format(x, y))

        bsubParams = {
            "nodes" : nodes,
            "mounts" : mounts,
            "arcsiContainer": mpiConfig["container"]["location"],
            "working": self.paths["working"],
            "tempOutdir": self.tempOutDir,
            "projabbv": self.projAbbv,
            "outWkt" : self.outWkt,
            "dem" : self.dem,
            "fileList" : self.fileListPath
        }

        bsub = template.substitute(bsubParams)
        
        #need realworld mapping of working path
        target = os.path.join(self.paths["working"], "run_arcsimpi.bsub")

        with open(target, 'w') as out:
            out.write(bsub)

        #todo swap working path (hostWorkingPath)
        #todo parameterise the bsub path?
        cmd = "/apps/lsf/10.1/linux2.6-glibc2.3-x86_64/bin/bsub -x < {}".format(target)
        
        if not self.testProcessing:
            try:
                log.info("Running cmd: " + cmd)

                subprocess.run(cmd, check=True, stderr=subprocess.STDOUT, shell=True)
                
            except subprocess.CalledProcessError as e:
                errStr = "command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output)
                log.error(errStr)
                raise RuntimeError(errStr)

        #todo: should be job number reall
        output = {
            "job" : "started"
        }
        
        with self.output().open('w') as o:
            json.dump(output, o, indent=4)

    def output(self):
        outFile = os.path.join(self.paths['state'], 'ProcessRawToArd.json')
        return LocalTarget(outFile)