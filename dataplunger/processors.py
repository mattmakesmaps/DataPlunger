"""
.. module:: processors.py
   :platform: Unix
   :synopsis: Processors perform changes on an entire set of records.

.. moduleauthor:: Matt

Processors perform actions on a collection records.
"""
__author__ = 'mkenny'
import abc
import csv
import itertools
import os
import readers


class ProcessorBaseClass(object):
    """
    Abstract Base Class implementing an interface for Processsors.

    Methods subclasses must override:

    - __init__(): takes an input processor to decorate, and any kwargs.
    - _process(): takes an iterable object of records and performs processing actions on them.

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

    def _log(self, mod_records_iterable):
        """
        Override to perform logging. Executed in process(),
        after protected method, _process() returns modified records iterable.

        :param mod_records_iterable: A list of records to log an action against.
        """
        pass

    @abc.abstractmethod
    def _process(self, records_iterable):
        """
        Returns an iterable object containing records, after having performed
        some processing actions on them.

        By convention, an iterable will be one of:

        - An instance of an iterator within the itertools standard lib.
        - A list.

        :param records_iterable: An iterable object of records to perform an action against.
        """
        mod_records_iterable = records_iterable
        return mod_records_iterable

    def process(self, records_iterable):
        """
        Call internal _process() method, then pass result to _log(),
        ending with a call to the decorated class' public process() method.

        :param records_iterable: An iterable object of records to perform an action against.
        """
        mod_records_iterable = self._process(records_iterable)
        self._log(mod_records_iterable)
        # Execute the process method only if processor has it.
        # Useful in case of ProcessorCombineData, where we create
        # an instance of ProcessorGetData w/o requiring a child to decorate.
        if hasattr(self.processor, "process"):
            self.processor.process(mod_records_iterable)
        return mod_records_iterable


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
        self.file = open(self.path, 'w')
        self.d_writer = csv.DictWriter(self.file, self.fields, extrasaction='ignore')

    def _log(self, mod_records_iterable):
        """Alert that CSV output is beginning."""
        print "Starting AggregateProcessorCSVWriter"

    def _write_row(self, row):
        self.d_writer.writerow(row)
        return row

    def _process(self, records_iterable):
        """Write inRecords out to a given CSV file"""
        self.d_writer.writeheader()
        write_record_iterator = itertools.imap(self._write_row, records_iterable)
        return write_record_iterator

    def __exit__(self):
        """
        Close File Handle
        """
        if os.path.isfile(self.file.name):
            if not self.file.closed:
                self.file.close()


class ProcessorDevNull(ProcessorBaseClass):
    """
    ProcessorDevNull serves as the last processor in the chain. It ends the
    processing chain by iterating through an iterable, ensuring that the last
    decorated iterable is executed.
    """
    def __init__(self):
        self.processor = None

    def _process(self, records_iterable):
        """
        Iterate through records to ensure that last decorated process is executed.
        This is required if last process returns an itertools class, as opposed to a list.
        """
        for record in records_iterable:
            pass
        # If given a list, will return contents.
        # If given an iterator, will return a spent iterator.
        return records_iterable

    def process(self, records_iterable):
        """
        Override to omit call to any additional processors.
        """
        mod_records_iterable = self._process(records_iterable)
        return mod_records_iterable


class ProcessorChangeCase(ProcessorBaseClass):
    """
    Responsible for changing case of values.

    Required Config Parameters:

    :param str case: Either "upper" or "lower".

    Example configuration file entry::

        {"ProcessorChangeCase": {"case": "upper"}}
    """
    def __init__(self, processor, case=None, **kwargs):
        self.processor = processor
        self.case = case.lower()

    def _change_case(self, dict_record):
        """
        Perform dictionary comprehension to change case based on user input.
        """
        if self.case == 'upper':
            #inLine = {key: value.upper() for key, value in inLine.iteritems() if isinstance(value, str)}
            dict_record.update((k, v.upper()) for k, v in dict_record.items() if isinstance(v, str))
        elif self.case == 'lower':
            #inLine = {key: value.lower() for key, value in inLine.iteritems() if isinstance(value, str)}
            dict_record.update((k, v.lower()) for k, v in dict_record.items() if isinstance(v, str))
        else:
            raise ValueError("Case Not Supported")
        return dict_record

    def _process(self, records_iterable):
        """Return an iterator of records mapped to _change_case"""
        change_case_iterator = itertools.imap(self._change_case, records_iterable)
        return change_case_iterator


class ProcessorGetData(ProcessorBaseClass):
    """
    Responsible for retrieving a generator from a given reader.

    Required Config Parameters:

    :param str reader: name of a given reader.

    Example configuration file entry::

        {"ProcessorGetData": {"reader": "Grades"}},
    """
    def __init__(self, processor, reader, readers, **kwargs):
        self.processor = processor
        self.reader_name = reader
        self.readers = readers

    def _get_reader_class(self):
        """
        Based on a Config Object's conn_info type attribute,
        generate an appropriate reader.
        """
        # Check if the configuration object contains a Reader type
        # we actually support. If so, build a reader.
        selected_reader = self.readers[self.reader_name]
        for reader_class in readers.ReaderBaseClass.__subclasses__():
            if selected_reader['type'] == reader_class.__name__:
                return reader_class
        raise TypeError("ERROR: %s is not a subclass of ReaderBaseClass" % reader_class)

    def _process(self, reader_name):
        """Return the generator for a given reader."""
        reader_class = self._get_reader_class()
        reader_kwargs = self.readers[reader_name]
        reader_instance = reader_class(**reader_kwargs)
        return reader_instance.__iter__()


class ProcessorCombineData(ProcessorBaseClass):
    """
    Joins records from an existing Reader+Processors to a new
    Reader. Will currently perform a LEFT JOIN only.

    Required Config Parameters:

    :param str reader: name of a given reader.
    :param list keys: list of field names to perform join on.

    Example configuration file entry::

        {"ProcessorCombineData": {"reader": "People", "keys": ["name"]}},
    """
    def __init__(self, processor, reader, keys, readers, **kwargs):
        self.processor = processor
        self.join_keys = keys
        self.new_reader_iter = ProcessorGetData(None, reader, readers).process(reader)


    def _merge_record(self, dict_record):
        """return a list of records sharing the same value for keys
        listed in self.keys"""
        key_count = len(self.join_keys)
        matching_record_found = False
        merged_records = []

        # Loop through combine set, looking for matching key:values.
        for record in self.new_record_iterable:
            # Get list of matching keys, check if len matches expected key count
            matching_keys = [k for k in self.join_keys if dict_record[k] == record[k]]
            if len(matching_keys) == key_count:
                matching_record_found = True
                merged_record = dict(dict_record.items() + record.items())
                merged_records.append(merged_record)

        # No matches, add the original record with empty values for expected keys.
        if not matching_record_found:
            # Create empty values for all keys except those responsible for joining
            # As they'll have populated values already.
            empty_keys = {k:'' for k in self.new_record_iterable_fields if k not in self.join_keys}
            merged_record = dict(dict_record.items() + empty_keys.items())
            merged_records.append(merged_record)

        return merged_records


    def _process(self, existing_record_iterable):
        """Return a generator that yields a the merged records
        from two readers"""
        # Create a list of items from new iterator
        self.new_record_iterable = [r for r in self.new_reader_iter]
        self.new_record_iterable_fields = self.new_record_iterable[0].keys()
        # Create a list of lists containing merged records
        merge_iterator = itertools.imap(self._merge_record, existing_record_iterable)
        # Flatten the list of lists.
        flatten_iterator = itertools.chain.from_iterable(merge_iterator)
        return flatten_iterator


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

    def _match_value(self, in_record):
        """
        Returns True or False. Iterate through our user-provided list of matches.
        If we find a match, take action specified by user.
        """
        match_found = False
        for match_key, match_value in self.matches.iteritems():
            if str(match_value) == str(in_record[match_key]):
                match_found = True

        if self.action == 'keep' and match_found is True:
            return True
        elif self.action == 'discard' and match_found is False:
            return True
        else:
            return False

    def _process(self, records_iterable):
        """Return an iterator mapped to _match_value() as a filter."""
        matched_iterator = itertools.ifilter(self._match_value, records_iterable)
        return matched_iterator


class ProcessorScreenWriter(ProcessorBaseClass):
    """
    A Processor class that simply prints a record's key, values.

    Required Config Parameters: **None**

    Example configuration file entry::

        {"ProcessorScreenWriter": null}
    """
    def __init__(self, processor, **kwargs):
        self.processor = processor

    def _print_line(self, dict_record):
        """Print a record to screen."""
        print dict_record
        return dict_record

    def _process(self, records_iterable):
        """Return an iterator mapped to _print_line()."""
        screen_writer_iterator = itertools.imap(self._print_line, records_iterable)
        return screen_writer_iterator


class ProcessorSortRecords(ProcessorBaseClass):
    """
    Perform ascending sort for a collection of records by a given key.

    Required Config Parameters:

    :param str sort_key: Field name (dict key) to sort by.

    Example configuration file entry::

        "ProcessorSortRecords": {
            "sort_key": "CITY_NAME"
        }
    """
    def __init__(self, processor, sort_key, **kwargs):
        self.processor = processor
        self.sort_key = sort_key

    def _process(self, records_iterable):
        """Return a list of sorted records using the builtin sorted() method."""
        sorted_records_list = sorted(records_iterable, key=lambda k: k[self.sort_key])
        return sorted_records_list


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
        self.out_fields = set(fields)

    def _truncate_line(self, dict_record):
        """
        Preform dict comprehension to create a dictionary subset to out_fields only.
        """
        return {r: dict_record[r] for r in self.out_fields}

    def _process(self, records_iterable):
        """Return an iterator mapped to _truncate_line()."""
        truncate_iterator = itertools.imap(self._truncate_line, records_iterable)
        return truncate_iterator
