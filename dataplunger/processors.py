__author__ = 'mkenny'
import abc
from c_processorchangecase import process_changecase

class ProcessorBaseClass(object):
    """
    Abstract Base Class implementing an interface for Processsors.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, processor, **kwargs):
        self.processor = processor

    def _log(self, inLine):
        #print 'DEFAULT log entry for class %s for record %s' % (self.__repr__(), inLine)
        pass

    def _process(self, inLine):
        self._log(inLine)
        # if self.processor:
        #     self.processor.process(inLine)
        self.process(inLine)

    @abc.abstractmethod
    def process(self, inLine):
        pass

class ProcessorDevNull(ProcessorBaseClass):
    """
    ProcessorDevNull serves as the last processor in the chain for a specific
    record. It ends the processing chain by appending the final record value
    back to the RecordConstructor's records list.
    """
    def __init__(self, RecordConstructor):
        self.processor = None
        self.record_constructor = RecordConstructor

    def process(self, inLine):
        self.record_constructor.records.append(inLine)

class ProcessorScreenWriter(ProcessorBaseClass):
    """
    A Processor class that simply prints contents of a line.
    """
    def __init__(self, processor, **kwargs):
        self.processor = processor

    def process(self, inLine):
        print inLine
        self.processor._process(inLine)


class ProcessorChangeCase(ProcessorBaseClass):
    """
    A Processor class which implements a public interface, the process() method.
    Responsible for changing case of values.
    """
    # NOTE: Passing a default value for self.case allows us
    # To not require it as an attribute for every layer.
    def __init__(self, processor, case=None, **kwargs):
        self.processor = processor
        self.case = case

    def _log(self, inLine):
        """
        This is an example of an overriden _log method
        """
        #print "OVERRIDDEN log for ProcessorChangeCase. Selected case: %s" % self.case
        pass

    def process(self, inLine):
        case = self.case.lower()
        if case not in {'lower', 'upper'}:
            raise ValueError("Case not supported.")
        case_mod_line = process_changecase(inLine, case)
        self.processor._process(case_mod_line)

class ProcessorTruncateFields(ProcessorBaseClass):
    """
    A decorator class which implements a Processor class' public
    interface, the process() method.
    """
    def __init__(self, processor, fields, **kwargs):
        self.processor = processor
        self.out_fields = fields

    def process(self, inLine):
        """/home/mkenny
        Perform dict comprehension to create a dictionary subset to out_fields only.
        """
        #truncated_line = {key: value for key, value in inLine.iteritems() if key in self.out_fields}
        truncated_line = {field: inLine[field] for field in self.out_fields}
        self.processor._process(truncated_line)


class ProcessorSortRecords(ProcessorBaseClass):
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
        self.processor._process(truncated_line)
