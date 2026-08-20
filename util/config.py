import json

configFilePath = "config.json"
config = None

def loadConfig():
    global config
    if config is None:
        with open(configFilePath, 'r') as f:
            config = json.load(f)
    return config