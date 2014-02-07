__author__ = 'mkenny'

from extracto import *
from pprint import pprint

if __name__ == '__main__':
    # Build a config object.
    config_path = '/home/mkenny/Projects/extracto-matic/scripts/sample_config.json'
    csvConfig = Configuration()
    csvConfig.parseConfig(config_path)

    # # Examine Contents
    # print "Layers:"
    # pprint(csvConfig.layers)
    # print "Connection Info"
    # pprint(csvConfig.conn_info)
    # print "========================"

    # Build a controller from the given configuration
    csvController = Controller(csvConfig)
    csvController.createRecordConstructors()
