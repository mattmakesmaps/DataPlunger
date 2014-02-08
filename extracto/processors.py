__author__ = 'mkenny'
import abc


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

    def process(self, inLine):
        # NOTE: Need to check for None type first.
        if self.case is None:
            self.processor._process(inLine)
        elif self.case.lower() == 'upper':
            inLine = {key: value.upper() for key, value in inLine.iteritems() if isinstance(value, str)}
            self.processor._process(inLine)
        elif self.case.lower() == 'lower':
            inLine = {key: value.lower() for key, value in inLine.iteritems() if isinstance(value, str)}
            self.processor._process(inLine)
        else:
            raise ValueError("Case Not Supported")

class ProcessorTruncateFields(ProcessorBaseClass):
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
