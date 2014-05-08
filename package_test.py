__author__ = 'mkenny'

from dataplunger import *
from datetime import datetime

if __name__ == '__main__':
    # Build a config object.
    start_time = datetime.now()
    config_path = '/home/matt/Projects/DataPlunger/sample_configs/multi_reader.json'
    collectionConfig = Configuration()
    collectionConfig.parse_config(config_path)

    combined = Controller(collectionConfig, "HeapSortConfig")
    combined.process_layers()
    combined_time = datetime.now()-start_time
    print "Combined Time: %s" % combined_time