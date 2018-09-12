import luigi
import os
import json
import process_s2_swath.common as common
from luigi import LocalTarget
from luigi.util import requires
from process_s2_swath.UnzipRaw import UnzipRaw
from process_s2_swath.GetInputFileInfo import GetInputFileInfo

@requires(UnzipRaw)
class GetInputFileInfos(luigi.Task):
    pathRoots = luigi.DictParameter()

    def run(self):
        with self.input().open('r') as unzipRawFile:
            unzipRawOutput = json.loads(unzipRawFile.read())

        tasks = []
        for product in unzipRawOutput["products"]:
            tasks.append(GetInputFileInfo(pathRoots=self.pathRoots, productPath=product))
        yield tasks

        products = []
        for task in tasks:
            with task.output().open('r') as taskOutput:
                submittedProduct = json.load(taskOutput)
                products.append(submittedProduct)

        output = {
            "products": products
        }

        with self.output().open("w") as outFile:
            outFile.write(common.getFormattedJson(output))

    def output(self):
        outFile = os.path.join(self.pathRoots['state'], 'GetInputFileInfos.json')
        return LocalTarget(outFile)