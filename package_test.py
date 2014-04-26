__author__ = 'mkenny'

from dataplunger import *
from pprint import pprint
from datetime import datetime

if __name__ == '__main__':
    # Build a config object.
    start_time = datetime.now()
    config_path = '/Users/matt/Projects/dataplunger/sample_configs/multi_reader.json'
    collectionConfig = Configuration()
    collectionConfig.parse_config(config_path)

    multireader = Controller(collectionConfig, "PostgresConfig")
    # multireader = Controller(collectionConfig, "SHPConfig")
    # multireader = Controller(collectionConfig, "WACensusConfig")
    # multireader = Controller(collectionConfig, "FreebaseConfig")
    multireader.process_layers()
    layer0_time = datetime.now()-start_time
    print "Layer Zero Time: %s" % layer0_time
