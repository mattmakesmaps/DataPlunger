"""
.. module:: processors.py
   :platform: Unix
   :synopsis: Processors perform changes on an entire set of records.

.. moduleauthor:: Matt

Processors perform changes on an entire set of records.
"""
__author__ = 'mkenny'
import abc
import csv

class ProcessorBaseClass(object):
    """
    Abstract Base Class implementing an interface for Processsors.

    Methods subclasses must override:

    - __init__(): takes an input processor to decorate, and any kwargs.
    - _process(): takes a list of records and performs processing actions on them.

    Methods subclasses can inherit (not required to override):

    - process(): responsible for calling _process(), _log(),
      then a decorated class' process() method.
    - _log(): Responsible for executing logging if overridden. Takes
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
        Returns a list of records after having performed some processing
        actions on them.

        :param inRecords: A list of records to log an action against.
        """
        mod_records = inRecords
        return mod_records

    def process(self, inRecords):
        """
        Call internal _process method, _log(), ending with a call
        to the next class' public process() method.

        :param inRecords: A list of records to log an action against.
        """
        modRecords = self._process(inRecords)
        self._log(modRecords)
        self.processor.process(modRecords)
        return modRecords


class ProcessorCSVWriter(ProcessorBaseClass):
    """
    Write records to an output CSV file.

    Required Config Parameters:

    :param str path: Absolute path for output CSV file.
    :param list fields: A list of field names to output.

    Example configuration file entry::

        {"ProcessorCSVWriter": {
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
        return inRecords


class ProcessorDevNull(ProcessorBaseClass):
    """
    ProcessorDevNull serves as the last processor in the chain. It ends the
    processing chain by appending the final aggregate modified
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

    def process(self, inRecords):
        """
        Override to omit call to any additional processors.
        """
        self._process(inRecords)


class ProcessorChangeCase(ProcessorBaseClass):
    """
    Responsible for changing case of values.

    Required Config Parameters:

    :param str case: Either "Upper" or "Lower".

    Example configuration file entry::

        {"ProcessorChangeCase": {"case": "upper"}}
    """
    def __init__(self, processor, case=None, **kwargs):
        self.processor = processor
        self.case = case.lower()

    def _change_case(self, inLine):
        """
        Perform dictionary comprehension to change case based on user input.
        """
        # # NOTE: Need to check for None type first.
        # if self.case is None:
        #     pass
        if self.case == 'upper':
            inLine = {key: value.upper() for key, value in inLine.iteritems() if isinstance(value, str)}
        elif self.case == 'lower':
            inLine = {key: value.lower() for key, value in inLine.iteritems() if isinstance(value, str)}
        else:
            raise ValueError("Case Not Supported")
        return inLine

    def _process(self, records):
        """Return a list of records with Case Changed"""
        mod_records = [self._change_case(record) for record in records]
        return mod_records


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

    def _match_value(self, inLine):
        """
        Iterate through our user-provided list of matches.
        If we find a match, take action specified by user.
        """
        match_found = False
        for match_key, match_value in self.matches.iteritems():
            if str(match_value) == str(inLine[match_key]):
                match_found = True

        if self.action == 'keep' and match_found is True:
            return True
        elif self.action == 'discard' and match_found is False:
            return True
        else:
            return False

    def _process(self, records):
        """
        Return a list of records based on a user's match criteria
        """
        matched_records = [r for r in records if self._match_value(r)]
        return matched_records


class ProcessorScreenWriter(ProcessorBaseClass):
    """
    A Processor class that simply prints a record's key, values.

    Required Config Parameters: **None**

    Example configuration file entry::

        {"ProcessorScreenWriter": null}
    """
    def __init__(self, processor, **kwargs):
        self.processor = processor

    def _process(self, records):
        """
        Print record to screen.
        """
        for record in records:
            print record
        return records


class ProcessorSortRecords(ProcessorBaseClass):
    """
    Perform ascending sort for a collection of records by a given key.

    Required Config Parameters:

    :param str sortby: Field name (dict key) to sort by.

    Example configuration file entry::

        "ProcessorSortRecords": {
            "sortby": "CITY_NAME"
        }
    """
    def __init__(self, processor, sortby, **kwargs):
        self.processor = processor
        self.sortby = sortby

    def _process(self, records):
        """Use the builtin sorted() method to asc sort by a given key"""
        sorted_records = sorted(records, key=lambda k: k[self.sortby])
        return sorted_records


class ProcessorTruncateFields(ProcessorBaseClass):
    """
    A decorator class which implements a Processor class' public
    interface, the process() method. Designed to truncate a record
    to a specific set of fields.

    Required Config Parameters:

    :param list fields: field names to keep.

    Example configuration file entry::

        {"ProcessorTruncateFields": {
                "fields": ["Total", "Male", "Female", "SUMLEVEL", "LOGRECNO"]
        }}
    """
    def __init__(self, processor, fields, **kwargs):
        self.processor = processor
        self.out_fields = fields

    def _truncate_line(self, inLine):
        """
        Preform dict comprehension to create a dictionary subset to out_fields only.
        """
        truncated_line = {key: value for key, value in inLine.iteritems() if key in self.out_fields}
        return truncated_line

    def _process(self, records):
        """
        Return a list of records truncated to self.out_fields
        """
        truncated_records = [self._truncate_line(record) for record in records]
        return truncated_records
