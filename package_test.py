__author__ = 'mkenny'

from dataplunger import *
from pprint import pprint
from datetime import datetime

if __name__ == '__main__':
    # Build a config object.
    start_time = datetime.now()
    config_path = '/Users/matt/Projects/dataplunger/test_configs/collection_config.json'
    collectionConfig = Configuration()
    collectionConfig.parseConfig(config_path)

    # # Examine Contents
    # print "Layers:"
    # pprint(csvConfig.layers)
    # print "Connection Info"
    # pprint(csvConfig.conn_info)
    # print "========================"

    # Build a controller from the given configuration
    electionController = Controller(collectionConfig, "KC Election Data")
    electionController.createRecordConstructors()
    layer_one_time = datetime.now()-start_time
    print "Layer One Time: %s" % layer_one_time
    resturantController = Controller(collectionConfig, "KC Restaurant Inspection Data")
    resturantController.createRecordConstructors()
    total_time = datetime.now()-start_time
    print "Total Time: %s" % total_time
