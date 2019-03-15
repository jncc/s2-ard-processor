import json
import luigi
import os

def getFormattedJson(jsonString):
    return json.dumps(jsonString, indent=4)

def makePath(self, newPath):
    if not os.path.exists(newPath):
        os.makedirs(newPath)

def clearFolder(self, folder):
    for f in os.listdir(folder):
        path = os.path.join(folder, f)

        if os.path.isfile(path):
            os.unlink(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)    