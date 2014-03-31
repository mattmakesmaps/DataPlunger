"""
.. module:: core.py
   :platform: Unix
   :synopsis: Contains configuration and control code.

.. moduleauthor:: Matt

Contains configuration and control code.
"""
__author__ = 'mkenny'
from .readers import *
from .processors import *
from simplejson import loads as json_loads


class Configuration(object):
    """
    This class is responsible the conversion of a JSON-formatted configuration
    file into a python object.

    :param str inConfigPath: The full pathway to a JSON-formatted configuration file.
    :param dict parsed_configs: Configuration info populated by parsedConfig.
    """

    def __init__(self):
        """
        Requires full pathway to configuration path.
        """
        self.inConfigPath = ''
        self.parsed_configs = {}

    def _get_config_data(self, inConfigPath):
        """
        Given a file path to a JSON config file, open and
        convert to a python object.

        :param str inConfigPath: full pathway to a JSON-formatted config file.
        """
        with open(inConfigPath) as config_file:
            config_string = config_file.read()
            config_data = json_loads(config_string)
            return config_data

    def _validate_config(self, config_data):
        """
        Not implemented.
        Validate we have all required attributes.

        :param dict config_data: parsed config info via _get_config_data()
        """
        return config_data

    def parseConfig(self, inConfigPath):
        """
        Given a configuration path, call methods to validate
        and parse the file into a useable python object.

        :param str inConifgPath: full pathway to a JSON-formatted config file.
        """
        parsed_configs = self._get_config_data(inConfigPath)
        if self._validate_config(parsed_configs):
            # Generate a dictionary of individual configs with a
            # ConfigCollection JSON object.
            self.parsed_configs = {
                config['name']: {'layers': config['layers'], 'conn_info': config['conn_info']}
                for config in parsed_configs['configs']
            }


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

    :param config: Config instance to be passed to the controller.
    :param config_name: Name of the individual config within a ConfigCollection to be processed.
    """

    def __init__(self, config, config_name):
        self.config_name = config_name 
        self.conn_info = config.parsed_configs[self.config_name]['conn_info']
        self.layers = config.parsed_configs[self.config_name]['layers']

    def _get_reader(self):
        """
        Based on a Config Object's conn_info type attribute,
        generate an appropriate reader.
        """
        # Check if the configuration object contains a Reader type
        # we actually support. If so, build a reader.
        for reader_class in ReaderBaseClass.__subclasses__():
            if self.conn_info['type'] == reader_class.__name__:
                return reader_class
        raise TypeError("ERROR: %s is not a subclass of ReaderBaseClass" % reader_class)

    def createRecordConstructors(self):
        """
        Create a Reader class instance.
        Create a RecordConstructor for each layer.
        Initiate processing calling the RecordConstructor's serialize() method.
        """
        # Get required reader class and create an instance.
        selectedReaderClass = self._get_reader()
        selectedReader = selectedReaderClass(self.conn_info)
        # Spawn RecordConstructors for each layer.
        for layer_name, layer_parameters in self.layers.iteritems():
            # Extract processing steps for a layer
            processing_steps = layer_parameters['processing_steps']
            rBuild_Inst = RecordConstructor(selectedReader, layer_name, layer_parameters, processing_steps)
            rBuild_Inst.serialize()


class RecordConstructor(object):
    """
    A RecordConstructor is composed of a reader and n-number of processors.
    The RecordConstructor is responsible for actually iterating through a datasource,
    provided by self.reader, and processing it using self.processors.

    :param reader: reader class responsible for connecting to a data source.
    :param layer_name: layer name extracted from a configuration file.
    :param layer_config_parameters: layer parameters extracted from a configuration file.
    :param processors: processor class references to be applied to a record.
    :param list records: processed record.
    """

    def __init__(self, reader, layer_name, layer_config_params, processors=None):
        self.reader = reader
        self.layer_name = layer_name
        self.layer_config_params = layer_config_params
        self.processors = processors

    def _get_processor_instance(self, name, base_class):
        """
        Given a string, test if a Processor class exists by that name.
        """
        # A valid processor should be an explicit subclass of ProcessorBaseClass
        for processor in base_class.__subclasses__():
            if name == processor.__name__:
                return processor
        raise TypeError("ERROR: %s processor does not exist" % name)

    def _build_decorated_classes(self, data, initial_processor, processors, BaseClass):
        """
        For Record and Aggregate processors, apply decorators and begin processing.
        """
        decorated_processor = initial_processor
        # Can pass **kwargs when instantiating a class. If that class contains a
        # property assigned at init sharing the name of a key in the **kwargs dict,
        # that property will be assigned the value extracted from **kwargs
        for processor_dict in processors:
            for processor_name, processor_args in processor_dict.iteritems():
                # Create an actual instance of the processor
                processor_instance = self._get_processor_instance(processor_name, BaseClass)
                if processor_args:
                    # update layer_config_params to include processor_args keys
                    self.layer_config_params.update(processor_args)
                decorated_processor = processor_instance(decorated_processor, **self.layer_config_params)
        decorated_processor.process(data)

    def serialize(self):
        """
        Using the reader classes' context manager, loop through
        a reader's contents using the reader's __iter__() method.
        Output is written (serialized) using the Processor class's
        process() method.
        """
        self.processors.reverse()
        with self.reader as local_reader_iter:
            # # Build Record List
            # for record in local_reader:
            #     self.records.append(record)
            # # Begin processing chain.
            # decorated_processor = ProcessorDevNull(self)
            # self._build_decorated_classes(self.records, decorated_processor, self.processors, ProcessorBaseClass)

            # Trying to pass the object since it is an iterator.
            decorated_processor = ProcessorDevNull()
            self._build_decorated_classes(local_reader_iter, decorated_processor, self.processors, ProcessorBaseClass)
