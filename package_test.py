__author__ = 'mkenny'

from dataplunger import *
from pprint import pprint
from datetime import datetime

if __name__ == '__main__':
    # Build a config object.
    start_time = datetime.now()
    config_path = '/users/matt/Projects/DataPlunger/sample_configs/multi_reader.json'
    collectionConfig = Configuration()
    collectionConfig.parse_config(config_path)

    # solos = Controller(collectionConfig, "USCensusConfigSolos")
    # combined = Controller(collectionConfig, "USCensusConfigCombined")
    heap = Controller(collectionConfig, "HeapSortConfig")
    # multireader = Controller(collectionConfig, "SHPConfig")
    # multireader = Controller(collectionConfig, "WACensusConfig")
    # multireader = Controller(collectionConfig, "FreebaseConfig")
    # solos.process_layers()
    # solos_time = datetime.now()-start_time
    # print "Solos Time: %s" % solos_time

    # combined.process_layers()
    # combined_time = datetime.now()-start_time
    heap.process_layers()
    combined_time = datetime.now()-start_time
    print "Combined Time: %s" % combined_time