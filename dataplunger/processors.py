"""
.. module:: processors.py
   :platform: Unix
   :synopsis: Processors perform changes on an individual record.

.. moduleauthor:: Matt

Processors perform changes on an individual record.
"""
__author__ = 'mkenny'
import abc


class ProcessorBaseClass(object):
    """
    Abstract Base Class implementing an interface for Processsors.

    Required methods for subclasses:

    - __init__(): takes an input processor to decorate, and any kwargs
    - _process(): takes a single parsed record as a dictionary of field names and field values.
      Method calls the 'process()' method of a decorated processor. Method returns the
      modified version of the record.

    Non-Required methods for subclasses:

    - process() - Responsible for calling _process() followed by _log().
    - _log() - Responsible for executing logging if overridden. Takes modified record as input.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, processor, **kwargs):
        self.processor = processor

    def _log(self, modLine):
        #print 'DEFAULT log entry for class %s for record %s' % (self.__repr__(), inLine)
        pass

    @abc.abstractmethod
    def _process(self, inLine):
        return self.processor.process(inLine)

    def process(self, inLine):
        modLine = self._process(inLine)
        self._log(modLine)
        return modLine  # return processed line, useful for testing.


class ProcessorMatchValue(ProcessorBaseClass):
    """
    Keep or discard a record that matches a user-defined field-value pair.
    A match will be subjected to the action specified in the "action" param.

    Required Config Parameters:

    :param dict matches: Field names (keys) and field entries (values).
    :param str action: Either "Keep" or "Discard". DEFAULTS to "Keep".

    If multiple matches are provided, a hit of any match will trigger the action.

    Example configuration file entry::

        {"ProcessorMatchValue": {
            "matches":{"SumLevel":140},
            "action":"Keep"
        }}
    """
    def __init__(self, processor, matches=None, action="Keep", **kwargs):
        self.processor = processor
        self.matches = matches
        self.action = action.lower()

    def _take_action(self, inLine, match_found):
        """
        If record meets criteria for inclusion, keep it processing.
        """
        if self.action == 'keep' and  match_found is True:
            self.processor.process(inLine)
            return True
        elif self.action == 'discard' and match_found is False:
            self.processor.process(inLine)
            return True
        else:
            return False

    def _process(self, inLine):
        """
        Iterate through our user-provided list of matches.
        If we find a match, take action specified by user.
        """
        match_found = False
        for match_key, match_value in self.matches.iteritems():
            if str(match_value) == str(inLine[match_key]):
                match_found = True
        if self._take_action(inLine, match_found):
            # Return inLine only if we have continued processing.
            # Used to validate function in test_processors.py
            return inLine


class ProcessorDevNull(ProcessorBaseClass):
    """
    ProcessorDevNull serves as the last processor in the chain for a specific
    record. It ends the processing chain by appending the final record value
    back to the RecordConstructor's records list.
    """
    def __init__(self, RecordConstructor):
        self.processor = None
        self.record_constructor = RecordConstructor

    def _process(self, inLine):
        """
        Add record to the associated record_constructor's record list.
        """
        self.record_constructor.records.append(inLine)


class ProcessorScreenWriter(ProcessorBaseClass):
    """
    A Processor class that simply prints a record's key, values.

    Required Config Parameters: **None**

    Example configuration file entry::

        {"ProcessorScreenWriter": null}
    """
    def __init__(self, processor, **kwargs):
        self.processor = processor

    def _process(self, inLine):
        """
        Print record to screen.
        """
        print inLine
        self.processor.process(inLine)
        return inLine


class ProcessorChangeCase(ProcessorBaseClass):
    """
    Responsible for changing case of values.

    Required Config Parameters:

    :param str case: Either "Upper" or "Lower".

    Example configuration file entry::

        {"ProcessorChangeCase": {"case": "upper"}}
    """
    # NOTE: Passing a default value for self.case allows us
    # To not require it as an attribute for every layer.
    def __init__(self, processor, case=None, **kwargs):
        self.processor = processor
        self.case = case

    def _log(self, modLine):
        """
        This is an example of an overriden _log method
        """
        #print "OVERRIDDEN log for ProcessorChangeCase. Selected case: %s" % self.case
        #print "Finished Line is %s" % modLine
        pass

    def _process(self, inLine):
        """
        Perform dictionary comprehension to change case based on user input.
        """
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
        return inLine

class ProcessorTruncateFields(ProcessorBaseClass):
    """
    A decorator class which implements a Processor class' public
    interface, the process() method. Designed to truncate a record
    to a specific set of fields.

    :param list fields: field names to keep.

    Example configuration file entry::

        {"ProcessorTruncateFields": {
                "fields": ["Total", "Male", "Female", "SUMLEVEL", "LOGRECNO"]
        }}
    """
    def __init__(self, processor, fields, **kwargs):
        self.processor = processor
        self.out_fields = fields

    def _process(self, inLine):
        """
        Perform dict comprehension to create a dictionary subset to out_fields only.
        """
        truncated_line = {key: value for key, value in inLine.iteritems() if key in self.out_fields}
        self.processor.process(truncated_line)
        return truncated_line
