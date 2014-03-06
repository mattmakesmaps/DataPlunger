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

        :param inRecods: A list of records to log an action against.
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

        :param inRecods: A list of records to log an action against.
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
        Reset the originial record_constructor's records list to the final list.
        """
        self.record_constructor.records = inRecords

class AggregateProcessorSortRecords(AggregateProcessorBaseClass):
    """
    A Processor class sorts a collection of records
    """
    def __init__(self, processor, sortby, **kwargs):
        self.processor = processor
        self.sortby = sortby

    def _process(self, records):
        sorted_records = sorted(records, key=lambda k: k[self.sortby])
        self.processor.process(sorted_records)


class AggregateProcessorScreenWriter(AggregateProcessorBaseClass):
    """
    A Processor class that simply prints contents of a line.
    """
    def __init__(self, processor, **kwargs):
        self.processor = processor

    def _process(self, inLine):
        print inLine
        self.processor.process(inLine)


class AggregateProcessorCSVWriter(AggregateProcessorBaseClass):
    """
    A Processor class that simply prints contents of a line.
    """
    def __init__(self, processor, path, fields, **kwargs):
        self.processor = processor
        self.path = path
        self.fields = fields

    def _log(self, inRecords):
        print "Starting AggregateProcessorCSVWriter"

    def _process(self, inRecords):
        with open(self.path, 'w') as file:
            dWriter = csv.DictWriter(file, self.fields, extrasaction='ignore')
            dWriter.writerows(inRecords)
        self.processor.process(inRecords)
