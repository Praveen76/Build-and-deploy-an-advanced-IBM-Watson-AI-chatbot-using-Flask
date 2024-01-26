import glob
import yaml

class ResourceReader:
    def read_configuration(self):
        with open("config/cbda.config", 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
            return cfg