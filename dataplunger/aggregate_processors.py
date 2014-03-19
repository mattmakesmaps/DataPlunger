"""
.. module:: aggregate_processors.py
   :platform: Unix
   :synopsis: Aggregate processors perform changes on an entire set of records.

.. moduleauthor:: Matt

Aggregate processors perform changes on an entire set of records.
"""
__author__ = 'mkenny'
import abc
import csv

class AggregateProcessorBaseClass(object):
    """
    Abstract Base Class implementing an interface for Aggregate Processsors.

    Methods subclasses must override:

    - __init__(): takes an input processor to decorate, and any kwargs.
    - _process(): takes a list of records, calls 'process()' method of
      decorated processor. Method returns modified list of records.

    Methods subclasses can inherit (not required to override):

    - process() - responsible for calling _process followed by _log().
    - _log() - Responsible for executing logging if overridden. Takes
      a list of records as input.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, processor, **kwargs):
        """
        :param processor: A decorated processor object to be executed.
        """
        self.processor = processor

    def _log(self, inRecords):
        """
        Responsible for executing logging if overridden.

        :param inRecords: A list of records to log an action against.
        """
        pass

    @abc.abstractmethod
    def _process(self, inRecords):
        """
        Perform an action against a list of records.

        :param inRecords: A list of records to log an action against.
        """
        return self.processor.process(inRecords)

    def process(self, inRecords):
        """
        Call _log() followed by process()

        :param inRecords: A list of records to log an action against.
        """
        modRecords = self._process(inRecords)
        self._log(modRecords)
        return modRecords


class AggregateProcessorDevNull(AggregateProcessorBaseClass):
    """
    ProcessorDevNull serves as the last processor in the chain for a specific
    record. It ends the processing chain by appending the final aggregate modified
    set of records back to the record constructor.
    """
    def __init__(self, RecordConstructor):
        self.processor = None
        self.record_constructor = RecordConstructor

    def _process(self, inRecords):
        """
        Reset the original record_constructor's records list to the final list.
        """
        self.record_constructor.records = inRecords


class AggregateProcessorSortRecords(AggregateProcessorBaseClass):
    """
    Perform ascending sort for a collection of records by a given key.

    Required Config Parameters:

    :param str sortby: Field name (dict key) to sort by.

    Example configuration file entry::

        "AggregateProcessorSortRecords": {
            "sortby": "CITY_NAME"
        }
    """
    def __init__(self, processor, sortby, **kwargs):
        self.processor = processor
        self.sortby = sortby

    def _process(self, records):
        """Use the builtin sorted() method to asc sort by a given key"""
        sorted_records = sorted(records, key=lambda k: k[self.sortby])
        self.processor.process(sorted_records)


class AggregateProcessorCSVWriter(AggregateProcessorBaseClass):
    """
    Write records to an output CSV file.

    Required Config Parameters:

    :param str path: Absolute path for output CSV file.
    :param list fields: A list of field names to output.

    Example configuration file entry::

        {"AggregateProcessorCSVWriter": {
            "path":"/path/to/out_data.csv",
            "fields": ["Age", "Gender", "Name"]
        }}

    """
    def __init__(self, processor, path, fields, **kwargs):
        self.processor = processor
        self.path = path
        self.fields = fields

    def _log(self, inRecords):
        """Alert that CSV output is beginning."""
        print "Starting AggregateProcessorCSVWriter"

    def _process(self, inRecords):
        """Write inRecords out to a given CSV file"""
        with open(self.path, 'w') as file:
            dWriter = csv.DictWriter(file, self.fields, extrasaction='ignore')
            dWriter.writerows(inRecords)
        self.processor.process(inRecords)
