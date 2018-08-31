import luigi
import json

def getFormattedJson(jsonString):
    return json.dumps(jsonString, indent=4)