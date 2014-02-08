__author__ = 'matt'
__date__ = '2/1/14'
"""
Tying together the concepts of the config parser, controller, reader, and processors.
"""
import csv
from simplejson import loads as json_loads
from pprint import pprint

"""
class AbstractProcessor(object):

    def __init__(self, inProcessor):
        self.processor = inProcessor

    def _log(self, stuff):
        pass

    def _process(self, inProcessor, inLine):
        _log(stuff)
        processor.process(inLine)

    def process(self, inLine):
        self._process(self.processor, inLine)
        # Note, subsequent children need to be rewritten to call _process rather then process.
"""

class Configuration(object):
    """
    This class is responsible the conversion of a JSON-formatted configuration
    file into a python object.

    self.inConfigPath - The full pathway to a JSON-formatted configuration file.
    self.conn_info - Extracted from a parsed configuration file.
                A dict representing relevant configuration information.
    self.layers - Extracted from a parsed configuration file.
                A dict with keys representing layer names and values representing
                properties for that layer.
    """

    def __init__(self):
        """
        Requires full pathway to configuration path.
        """
        self.inConfigPath = ''
        self.conn_info = {}
        self.layers = {}

    def _get_config_data(self, inConfigPath):
        """
        Given a file path to a JSON config file, open and
        convert to a python object.
        """
        with open(inConfigPath) as config_file:
            config_string = config_file.read()
            config_data = json_loads(config_string)
            return config_data

    def _validate_config(self, config_data):
        """
        Not implemented.
        Validate we have all required attributes.
        """
        return config_data

    def parseConfig(self, inConfigPath):
        """
        Given a configuration path, call methods to validate
        and parse the file into a useable python object.
        """
        config_data = self._get_config_data(inConfigPath)
        if self._validate_config(config_data):
            # Populate layers and conn_info attributes
            self.layers = config_data['layers']
            self.conn_info = config_data['conn_info']


class ProcessorScreenWriter(object):
    """
    A Processor class that simply prints contents of a line.
    """
    def __init__(self, processor, **kwargs):
        self.processor = processor

    def process(self, inLine):
        print inLine
        self.processor.process(inLine)


class ProcessorChangeCase(object):
    """
    A Processor class which implements a public interface, the process() method.
    Responsible for changing case of values.
    """
    # NOTE: Passing a default value for self.case allows us
    # To not require it as an attribute for every layer.
    def __init__(self, processor, case=None, **kwargs):
        self.processor = processor
        self.case = case

    def process(self, inLine):
        # NOTE: Need to check for None type first.
        if self.case is None:
            self.processor.process(inLine)
        elif self.case.lower() == 'upper':
            inLine = {key: value.upper() for key, value in inLine.iteritems() if isinstance(value, str)}
            self.processor.process(inLine)
        elif self.case.lower() == 'lower':
            inLine = {key: value.lower() for key, value in inLine.iteritems() if isinstance(value, str)}
            self.processor.process(inLine)
        else:
            raise ValueError("Case Not Supported")

class ProcessorTruncateFields(object):
    """
    A decorator class which implements a Processor class' public
    interface, the process() method.
    """
    def __init__(self, processor, fields, **kwargs):
        self.processor = processor
        self.out_fields = fields

    def process(self, inLine):
        """
        Perform dict comprehension to create a dictionary subset to out_fields only.
        """
        truncated_line = {key: value for key, value in inLine.iteritems() if key in self.out_fields}
        self.processor.process(truncated_line)

class DevNull(object):
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

    self.config - The Config instance to be passed to the controller.
    self.reader - Initially set to None, assigned in _get_reader()
    self.reader_map - A mapping of 'type' values in a JSON config file to
        an actual Reader class implementations.
    """

    def __init__(self, inConfigObject):
        self.config = inConfigObject
        self.reader = None
        self.reader_map = {
            'csv': CSVReader
        }

    def _get_reader(self):
        """
        Based on a Config Object's conn_info type attribute,
        generate an appropriate reader.
        """
        # Check if the configuration object contains a Reader type
        # we actually support. If so, build a reader.
        if self.config.conn_info['type'] in self.reader_map:
            self.reader = self.reader_map[self.config.conn_info['type']]
        else:
            raise KeyError("ERROR: Config file contains unsupported conn_info type")

    def createRecordConstructors(self):
        """
        Create a Reader class instance.
        Create a RecordConstructor for each layer.
        Initiate processing calling the RecordConstructor's serialize() method.
        """
        # Get required reader class and create an instance.
        self._get_reader()
        selectedReader = self.reader(self.config.conn_info)
        # Spawn RecordConstructors for each layer.
        for layer_name, layer_parameters in self.config.layers.iteritems():
            # Extract processing steps for a layer
            processing_steps = layer_parameters['processing_steps']
            rBuild_Inst = RecordConstructor(selectedReader, layer_name, layer_parameters, processing_steps)
            rBuild_Inst.serialize()


class CSVReader(object):
    """
    Reader class implementation for CSV files.

    self.conn_info - the connection information (pathway) for a given file.
    self._file_handler - set in __enter__(), a read only pointer to the CSV.
    self._dict_reader - an instance of csv.dict_reader()
    """
    def __init__(self, conn_info):
        self.conn_info = conn_info
        self._file_handler = None
        self._dict_reader = None

    def __enter__(self):
        """
        Open a file connection, pass that to an instance of csv.DictReader
        """
        self._file_handler = open(self.conn_info['path'], 'rt')
        self._dict_reader = csv.DictReader(self._file_handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # http://www.itmaybeahack.com/book/python-2.6/html/p03/p03c07_contexts.html
        # Close the file handler.
        self._file_handler.close()
        if exc_type is not None:
            # Exception occurred
            return False # Will raise the exception
        return True # Everything's okay

    def __iter__(self):
        # Generator returning a dict of field name: field value pairs for each record.
        for row in self._dict_reader:
            yield row

class RecordConstructor(object):
    """
    A RecordConstructor is composed of a reader and n-number of processors.
    The RecordConstructor is responsible for actually iterating through a datasource,
    provided by self.reader, and processing it using self.processors.

    self.reader - the Reader class responsible for connecting to a data source.
    self.layer_name - the layer name extracted from a configuration file.
    self.layer_config_parameters - any layer parameters extracted from a configuration file.
    self.processors - Processor class references to be applied to a record.
    """
    def __init__(self, reader, layer_name, layer_config_params, processors=None):
        self.reader = reader
        self.layer_name = layer_name
        self.layer_config_params = layer_config_params
        self.processors = processors

    def _get_processor_instance(self, processor_name):
        """
        Given a string, test if a Processor class exists by that name.
        TODO: This seems like a __bad__ idea.
        """
        if processor_name in globals():
            return globals()[processor_name]

    def serialize(self):
        """
        Using the reader classes' context manager, loop through
        a reader's contents using the reader's __iter__() method.
        Output is written (serialized) using the Processor class's
        process() method.
        """
        self.processors.reverse()
        with self.reader as local_reader:
            for record in local_reader:
                decorated_processor = DevNull()
                for processor_dict in self.processors:
                    # Can pass **kwargs when instanciating a class. If that class contains a
                    # property assigned at init sharing the name of a key in the **kwargs dict,
                    # that property will be assigned the value extracted from **kwargs
                    for processor_name, processor_args in processor_dict.iteritems():
                        # Create an actual instance of the processor
                        processor_instance = self._get_processor_instance(processor_name)
                        # update layer_config_params to include processor_args keys
                        if processor_args:
                            self.layer_config_params.update(processor_args)
                        decorated_processor = processor_instance(decorated_processor, **self.layer_config_params)
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

    # Build a controller from the given configuration
    csvController = Controller(csvConfig)
    csvController.createRecordConstructors()
