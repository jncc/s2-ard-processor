import os
import logging
import re
import shutil
import glob

from functional import seq
from xml.etree import ElementTree
from pathlib import Path

#testing
import json
import pprint

log = logging.getLogger("luigi-interface")

class SplitGranuleHandler():

    def copy(self, src, dest):
        try:
            shutil.copytree(src, dest)
        except OSError as e:
            # If the error was caused because the source wasn't a directory
            if e.errno == errno.ENOTDIR:
                shutil.copy(src, dest)
            else:
                print('Directory not copied. Error: %s' % e)

    def getGranuleName(self, data, index):
        return data["gId"][:-1] + "SPLIT" + str(index) + "_" + data["captureDate"]

    def MoveGranule(self, data, index):
        #Create new folder name
        newFolder = self.getGranuleName(data, index)
        data["newGranulePath"] = os.path.join(os.path.dirname(data["granulePath"]), newFolder)
        self.copy(data["granulePath"], data["newGranulePath"])

    def ModifyMetadata(self, data, index):
        metadataPath = os.path.join(data["newGranulePath"], "MTD_MSIL1C.xml")

        if not Path(metadataPath).is_file():
            raise Exception("No metadata found at", metadataPath)

        xml = ElementTree.parse(metadataPath)

        for x in xml.iter("PRODUCT_URI"):
            x.text = self.getGranuleName(data, index) + ".SAFE"

        xml.write(metadataPath)

    def DeleteOldGranule(self, data):
        try:
            shutil.rmtree(data["granulePath"])
        except Exception as e:
            log.warning("Unable to delete renamed split granule %s, error: %s", data["granulePath"], e)


    def IdentifySplitGranules(self, granules):
        # Create an array of granule data containing the graual path,
        #  first (static) part of the name and the capture data (which is what differes between split granules) 
        # =========================================================================================

        # map each source granule path to a reg ex match and return a tupple of the source path and reg ex matches
        #   of the static part of the name and capture date
        # Create an array of objects containg this data
        # Group each of the objects by the first part of the granule name (gId). This gives a 
        #    tuple of form (gid. list of corrisponding objects)
        # Take only those groupings where there is more then one object for the gid
        # Create a list of objects in each split and order it by captureDate to create a list of these ordered groupled lists

        pattern = "(S2[AB]_MSIL1C_\d{8}T\d{6}_[A-Z\d]+_[A-Z\d]+_[A-Z\d]+_)(\d{8}T\d{6})"

        splits = seq(granules) \
                    .map(lambda x: (x, re.match(pattern, os.path.basename(x)))) \
                    .select(lambda x: {
                            "granulePath" : x[0],
                            "newGranulePath" : "",
                            "gId": x[1][1], 
                            "captureDate": x[1][2]
                    }) \
                    .group_by(lambda x: x["gId"]) \
                    .filter(lambda x: len(x[1]) > 1) \
                    .select(lambda x: 
                        seq(x[1]) \
                        .order_by(lambda x: x["captureDate"])) \
                    .to_list()
        
        return splits

    def handleSplitGranules(self, granules):
        splits = self.IdentifySplitGranules(granules)

        if len(splits) == 0:
            log.info("No split granules detected")
            return granules

        output = []

        output.extend(granules)

        for s in splits:
            i = 1
            for g in s:
                self.MoveGranule(g, i)
            
                self.ModifyMetadata(g, i)
                self.DeleteOldGranule(g)

                output.remove(g["granulePath"])
                output.append(g["newGranulePath"])

                i += 1

        print ("output")
        return output




