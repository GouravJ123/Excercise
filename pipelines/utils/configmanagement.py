
import os
import json

def loadConfigFile():
    dirname = os.path.dirname(__file__)
    configFile = os.path.join(dirname, '../../configs/config.json')
    with open(configFile, 'r') as f:
        config = json.load(f)
    return config

def getConfig():
    config = loadConfigFile()
    return config