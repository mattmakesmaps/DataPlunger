__author__ = 'mkenny'
import abc

class AggregateProcessorBaseClass(object):
    """
    Abstract Base Class implementing an interface for Aggregate Processsors.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, processor, **kwargs):
        self.processor = processor

    def _log(self, inRecords):
        print 'DEFAULT log entry for class %s' % (self.__repr__())

    def _process(self, inRecords):
        self._log(inRecords)
        self.process(inRecords)

    @abc.abstractmethod
    def process(self, inRecords):
        pass

class AggregateProcessorDevNull(AggregateProcessorBaseClass):
    """
    ProcessorDevNull serves as the last processor in the chain for a specific
    record. It ends the processing chain by appending the final aggregate modified
    set of records back to the record constructor.
    """
    def __init__(self, RecordConstructor):
        self.processor = None
        self.record_constructor = RecordConstructor

    def process(self, inRecords):
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

    def process(self, records):
        sorted_records = sorted(records, key=lambda k: k[self.sortby])
        self.processor._process(sorted_records)


class AggregateProcessorScreenWriter(AggregateProcessorBaseClass):
    """
    A Processor class that simply prints contents of a line.
    """
    def __init__(self, processor, **kwargs):
        self.processor = processor

    def process(self, inLine):
        print inLine
        self.processor._process(inLine)
