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

    # # Examine Contents
    # print "Layers:"
    # pprint(csvConfig.layers)
    # print "Connection Info"
    # pprint(csvConfig.conn_info)
    # print "========================"

    ## Build a controller from the given configuration
    # epaController = Controller(collectionConfig, "EPA FRS")
    # epaController.createRecordConstructors()
    # layer0_time = datetime.now()-start_time
    # print "Layer Zero Time: %s" % layer0_time
    # epaController1 = Controller(collectionConfig, "EPA FRS1")
    # epaController1.createRecordConstructors()
    # total_time = datetime.now()-start_time
    # print "Total Time: %s" % total_time
    # sex_by_age = Controller(collectionConfig, "Sex By Age")
    # sex_by_age.createRecordConstructors()
    multireader = Controller(collectionConfig, "PeopleAndGradesConfig")
    multireader.process_layers()
