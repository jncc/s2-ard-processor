import logging
import re

from functional import seq

log = logging.getLogger("luigi-interface")

class SplitGranuleHandler():

    def identifySplitGranules(self, granules):
        # Create an array of granule data containing the granule name,
        #  first (static) part of the name and the capture data (which is what differes between split granules) 
        # =========================================================================================

        # map each source granule name to a reg ex match and return a tupple of the source name and reg ex matches
        #   of the static part of the name and capture date
        # Create an array of objects containg this data
        # Group each of the objects by the first part of the granule name (gId). This gives a 
        #    tuple of form (gid. list of corrisponding objects)
        # Take only those groupings where there is more then one object for the gid
        # Create a list of objects in each split and order it by captureDate to create a list of these ordered groupled lists

        # Expecting names like SEN2_20220127_latn572lonw0037_T30VVJ_ORB080_20220127121733_utm30n_osgb
        pattern = "(SEN2_\d{8}_[a-z0-9]+_([A-Z\d]+)_[A-Z\d]+_)(\d{14}_)[a-zA-Z\d]+_[a-zA-Z\d]+"

        log.info("granules")
        for g in granules:
            log.info(g)

        splits = seq(granules) \
                    .map(lambda x: (x["productName"], re.match(pattern, x["productName"]))) \
                    .select(lambda x: {
                            "granuleName" : x[0],
                            "gId": x[1].group(1),
                            "tileId": x[1].group(2),
                            "captureDate": x[1].group(3)
                    }) \
                    .group_by(lambda x: x["gId"]) \
                    .filter(lambda x: len(x[1]) > 1) \
                    .select(lambda x: 
                        seq(x[1]) \
                        .order_by(lambda x: x["captureDate"])) \
                    .to_list()
        
        return splits

    def getSplitName(self, granule, splitNo):
        # Add SPLITX to tile ID
        newTileID = f'{granule["tileId"]}SPLIT{splitNo}'
        splitName = granule["granuleName"].replace(granule["tileId"], newTileID)

        # Remove acquisition date
        splitName = splitName.replace(granule["captureDate"], "")

        return splitName

    def getSplitGranuleNames(self, granules):
        output = {}

        splits = self.identifySplitGranules(granules)
        if len(splits) == 0:
            log.info("No split granules detected")
        else:
            for s in splits:
                i = 0
                for g in s:
                    #skip the first granule
                    if i > 0:
                        splitName = self.getSplitName(g, i)
                        output[g["granuleName"]] = splitName

                    i += 1

        return output




