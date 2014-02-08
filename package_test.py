__author__ = 'mkenny'

from dataplunger import *
from pprint import pprint
from datetime import datetime

if __name__ == '__main__':
    # Build a config object.
    start_time = datetime.now()
    config_path = '/Users/matt/Projects/extracto-matic/scripts/sample_config.json'
    csvConfig = Configuration()
    csvConfig.parseConfig(config_path)

    # Examine Contents
    print "Layers:"
    pprint(csvConfig.layers)
    print "Connection Info"
    pprint(csvConfig.conn_info)
    print "========================"

    # Build a controller from the given configuration
    csvController = Controller(csvConfig)
    csvController.createRecordConstructors()
    end_time = datetime.now()-start_time
    print "Total Time: %s" % end_time
