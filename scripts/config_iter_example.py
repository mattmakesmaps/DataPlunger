__author__ = 'matt'
__date__ = '2/1/14'
"""
Tying together the concepts of the config parser, controller, reader, and processors.
"""
import csv
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


class ProcessorScreenWriter:
    """
    A class which implements a file-like object's process() method.
    Simply prints contents of a line.
    """
    def __init__(self, processor, **kwargs):
        self.processor = processor

    def process(self, inLine):
        print inLine
        self.processor.process(inLine)


class ProcessorChangeCase:
    """
    A decorator class which implements a Processor class' public
    interface, the process() method.
    """
    def __init__(self, processor, case, **kwargs):
        self.processor = processor
        self.case = case

    def process(self, inLine):
        if self.case.lower() == 'upper':
            self.processor.process(inLine.upper())
        elif self.case.lower() == 'lower':
            self.processor.process(inLine.lower())
        else:
            raise ValueError("Case Not Supported")

class ProcessorTruncateFields:
    """
    A decorator class which implements a Processor class' public
    interface, the process() method.
    """
    def __init__(self, processor, fields, **kwargs):
        self.processor = processor
        self.out_fields = fields

    def process(self, inLine):
        """
        Perform dict comprehension given a list of fields
        """
        truncated_line = {key: value for key, value in inLine.iteritems() if key in self.out_fields}
        self.processor.process(truncated_line)

class DevNull:
    """
    A decorator class intended to be implemented at the last step
    of the processing chain. simply ends the workflow.
    """
    def __init__(self, **kwargs):
        pass

    def process(self, inLine):
        pass


class NoProcessorException(Exception):
    """
    An exception raised when no processors are passed to
    the controller.
    """
    pass


class Controller(object):
    """
    Given a configuration object, manage the creation of
    RecordConstructor instances, one for each layer.

    NOTE: This now assumes that the same processes will be applied
    to all layers in a configuration file.
    """

    def __init__(self, inConfigObject, processors=None):
        self.config = inConfigObject
        self.reader = None
        if processors is None:
            raise NoProcessorException("ERROR: No Processors Found.")
        else:
            # Add an instance of ProcessorDevNull, ensuring process ends.
            self.processors = processors
            self.processors.reverse()

    def __get_reader(self):
        """
        Based on a Config Object's conn_info type attribute,
        generate an appropriate reader.
        """
        self.reader_map = {
            'csv': CSVReader
        }

        # Check if the configuration object contains a Reader type
        # we actually support. If so, build a reader.
        if self.config.conn_info['type'] in self.reader_map:
            self.reader = self.reader_map[self.config.conn_info['type']]

    def createRecordConstructors(self):
        # Get required reader
        self.__get_reader()
        selectedReader = self.reader(self.config.conn_info)
        # Spawn RecordConstructors for each layer.
        for layer_name, layer_parameters in self.config.layers.iteritems():
            rBuild_Inst = RecordConstructor(selectedReader, layer_name, layer_parameters, self.processors)
            rBuild_Inst.serialize()


class CSVReader(object):
    """
    Provides methods to reader through a CSV file.
    """
    def __init__(self, conn_info):
        self.conn_info = conn_info
        self._file_handler = None
        self._dict_reader = None

    def __enter__(self):
        """
        Open the connection
        """
        self._file_handler = open(self.conn_info['path'], 'rt')
        self._dict_reader = csv.DictReader(self._file_handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # http://www.itmaybeahack.com/book/python-2.6/html/p03/p03c07_contexts.html
        self._file_handler.close()
        if exc_type is not None:
            # Exception occurred
            return False # Will raise the exception
        return True # Everything's okay

    def __iter__(self):
        for row in self._dict_reader:
            yield row

class RecordConstructor(object):
    """
    A RecordConstructor is composed of a reader
    and n-number of processors.
    """
    def __init__(self, reader, layer_name, layer_parameters, processors=None):
        self.reader = reader
        self.layer_name = layer_name
        self.layer_parameters = layer_parameters
        self.processors = processors

    def serialize(self):
        """
        Using the reader classes' context manager, loop through
        a reader's contents using the reader's __iter__() method.
        Output is written (serialized) using the Processor class's
        process() method.
        """
        with self.reader as local_reader:
            for record in local_reader:
                decorated_processor = DevNull()
                for processor in self.processors:
                    # Can pass **kwargs when instanciating a class. If that class contains a
                    # property assigned at init sharing the name of a key in the **kwargs dict,
                    # that property will be assigned the value extracted from **kwargs
                    decorated_processor = processor(decorated_processor, **self.layer_parameters)
                decorated_processor.process(record)

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
    print "========================"

    processingSteps = [
        # ProcessorScreenWriter,
        ProcessorTruncateFields,
        ProcessorScreenWriter
    ]

    # Build a controller from the given configuration
    csvController = Controller(csvConfig, processingSteps)
    csvController.createRecordConstructors()

    # manual_csv_config = {
    #     "type": "csv",
    #     "path": "/Users/matt/Projects/extracto-matic/scripts/sample_data.csv",
    #     "delimeter": ",",
    #     "encoding": "UTF-8"
    # }
    #
    # print "test CSV manually"
    # with CSVReader(manual_csv_config) as test_CSVReader:
    #     for record in test_CSVReader:
    #         print record
