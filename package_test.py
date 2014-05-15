__author__ = 'mkenny'

from dataplunger import *
from datetime import datetime


def execute_run(config_name, config_path):
    """Execute a processing run given a config name and pathway"""
    print "Processing Layer: %s" % config_name
    start_time = datetime.now()

    configuration = Configuration()
    configuration.parse_config(config_path)
    controller = Controller(configuration, config_name)
    controller.process_layers()

    finish_time = datetime.now()-start_time
    print "Layer Time: %s" % finish_time

if __name__ == '__main__':
    configs = [
        ('StarHeapSortConfig', '/Users/matt/Projects/dataplunger/sample_configs/multi_reader.json'),
    ]

    total_begin_time = datetime.now()
    for config in configs:
        execute_run(config[0], config[1])
    total_end_time = datetime.now()
    print "Total Execution Time: %s" % total_end_time
