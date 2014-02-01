__author__ = 'matt'
__date__ = '2/1/14'
"""
An example workflow to parse a configuration file formatted as JSON
into something useable in Python.
"""
from simplejson import loads as json_loads
from pprint import pprint


class Configuration(object):
    """
    Parse a configuration file.
    """

    def __init__(self):
        """
        Requires full pathway to conifguration path.
        """
        self.inConfigPath = ''
        self.conn_info = {}
        self.layers = {}

    def __get_config_data(self, inConfigPath):
        """
        Given a pathway, return a python object
        built from a JSON object.
        """
        with open(inConfigPath) as config_file:
            config_string = config_file.read()
            config_data = json_loads(config_string)
            return config_data

    def __validate_config(self, config_data):
        """
        Not implemented.
        Validate we have all required attributes.
        """
        return config_data

    def parseConfig(self, inConfigPath):
        """
        Given a configuration path, parse the file into
        a useable python object.
        """
        config_data = self.__get_config_data(inConfigPath)
        if self.__validate_config(config_data):
            # Populate layers and conn_info attributes
            self.layers = config_data['layers']
            self.conn_info = config_data['conn_info']


if __name__ == '__main__':
    # Build a config object.
    config_path = '/Users/matt/Projects/extracto-matic/scripts/sample_config.json'
    csvConfig = Configuration()
    csvConfig.parseConfig(config_path)

    # Examine Contents
    print "Layers:"
    pprint(csvConfig.layers)
    print "Connection Info"
    pprint(csvConfig.conn_info)

