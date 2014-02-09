__author__ = 'mkenny'

from dataplunger import *
from pprint import pprint
from datetime import datetime

if __name__ == '__main__':
    # Build a config object.
    start_time = datetime.now()
    config_path = '/Users/matt/Projects/dataplunger/sample_configs/epa_frs_config.json'
    collectionConfig = Configuration()
    collectionConfig.parseConfig(config_path)

    # # Examine Contents
    # print "Layers:"
    # pprint(csvConfig.layers)
    # print "Connection Info"
    # pprint(csvConfig.conn_info)
    # print "========================"

    # Build a controller from the given configuration
    epaController = Controller(collectionConfig, "EPA FRS")
    epaController.createRecordConstructors()
    layer0_time = datetime.now()-start_time
    print "Layer Zero Time: %s" % layer0_time

    epaController1 = Controller(collectionConfig, "EPA FRS1")
    epaController1.createRecordConstructors()
    layer1_time = datetime.now()-start_time
    print "Layer 1 Time: %s" % layer1_time

    epaController2 = Controller(collectionConfig, "EPA FRS2")
    epaController2.createRecordConstructors()
    layer2_time = datetime.now()-start_time
    print "Layer 2 Time: %s" % layer2_time

    epaController3 = Controller(collectionConfig, "EPA FRS3")
    epaController3.createRecordConstructors()
    layer3_time = datetime.now()-start_time
    print "Layer 3 Time: %s" % layer3_time

    epaController4 = Controller(collectionConfig, "EPA FRS4")
    epaController4.createRecordConstructors()
    layer4_time = datetime.now()-start_time
    print "Layer 4 Time: %s" % layer4_time

    total_time = datetime.now()-start_time
    print "Total Time: %s" % total_time
