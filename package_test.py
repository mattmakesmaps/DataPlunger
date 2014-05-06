__author__ = 'mkenny'

from dataplunger import *
from pprint import pprint
from datetime import datetime

if __name__ == '__main__':
    # Build a config object.
    start_time = datetime.now()
    config_path = '/home/mkenny/Projects/DataPlunger/sample_configs/us_all_census.json'
    collectionConfig = Configuration()
    collectionConfig.parse_config(config_path)

    solos = Controller(collectionConfig, "USCensusConfigSolos")
    combined = Controller(collectionConfig, "USCensusConfigCombined")
    # multireader = Controller(collectionConfig, "SHPConfig")
    # multireader = Controller(collectionConfig, "WACensusConfig")
    # multireader = Controller(collectionConfig, "FreebaseConfig")
    # solos.process_layers()
    # solos_time = datetime.now()-start_time
    # print "Solos Time: %s" % solos_time

    combined.process_layers()
    combined_time = datetime.now()-start_time
    print "Combined Time: %s" % combined_time
