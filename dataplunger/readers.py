"""
.. module:: readers.py
   :platform: Unix
   :synopsis: Readers connect to data and return individual records.

.. moduleauthor:: Matt

Readers are responsible for creating a connection to a backing datasource,
and returning an iterable that yields a single record of data from that datasource.
"""
__author__ = 'mkenny'
import abc
import csv
import os


class ReaderBaseClass(object):
    """
    An abstract base class for a Reader interface.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, conn_info):
        self.conn_info = conn_info

    @abc.abstractmethod
    def __enter__(self):
        pass

    @abc.abstractmethod
    def __exit__(self, exc_type, exc_val, ext_tb):
        pass

    @abc.abstractmethod
    def __iter__(self):
        pass


class ReaderCSV(ReaderBaseClass):
    """
    Reader class implementation for CSV files.
    
    Required Config Parameters:
    
    :param conn_info: the connection information (pathway) for a given CSV file.
    :param conn_info.path: attribute containing the actual file path.

    Example configuration file entry::

        {
        "name": "KC Election Data",
        "conn_info": {
            "type": "ReaderCSV",
            "path": "/Users/matt/Projects/dataplunger/sample_data/election_2010_kc.csv",
            "delimeter": ",",
            "encoding": "UTF-8"
        }
    """
    def __init__(self, conn_info):
        """
        :param conn_info: the connection information (pathway) for a given file.
        :param delimiter:  extracted from conn_info, defaults to ','
        :param _file_handler:  set in __enter__(), a read only pointer to the CSV.
        :param _dict_reader:  an instance of csv.dict_reader()

        """
        self.conn_info = conn_info
        # If no delimiter given in config, default to ','
        self.delimiter = conn_info.get('delimiter', ',')
        self._file_handler = None
        self._dict_reader = None



    def __enter__(self):
        """
        Open a file connection, pass that to an instance of csv.DictReader
        """
        self._file_handler = open(self.conn_info['path'], 'rt')
        self._dict_reader = csv.DictReader(self._file_handler, delimiter=self.delimiter)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close the file handler.
        """
        self._file_handler.close()
        if exc_type is not None:
            # Exception occurred
            return False  # Will raise the exception
        return True  # Everything's okay

    def __iter__(self):
        """
        Generator returning a dict of field name: field value pairs for each record.
        """
        for row in self._dict_reader:
            yield row


class ReaderCensus(ReaderBaseClass):
    """
    Reader class implementation for Census ACS Summary Files.

    Required Config Parameters:

    :param conn_info: contains path and sequence attributes (see below).
    :param conn_info.path: contains a pathway to an ACS formatted directory of estimate and geography tables.
    :param conn_info.sequence: Sequence No. From, 'Sequence_Number_and_Table_Number_Lookup.xls'
    :param conn_info.starting_position: Starting Position for table of int. From, 'Sequence_Number_and_Table_Number_Lookup.xls'
    :param conn_info.fields: an array of field names with line numbers. From, 'Sequence_Number_and_Table_Number_Lookup.xls'

    Example configuration file entry::

        {
        "name": "Sex By Age",
        "conn_info": {
            "type": "ReaderCensus",
            "path": "/path/to/folder/of/EstimatesAndGeographyTables",
            "delimiter": ",",
            "encoding": "UTF-8",
            "fields": {
                "Total": 1,
                "Male": 2,
                "Female": 17
            },
            "sequence": 2,
            "starting_position": 87
        }
    """

    def __init__(self, conn_info):
        """
        :param conn_info: contains path and sequence attributes (see above).
        :param delimiter: extracted from conn_info, defaults to ','.
        :param _estimate_reader: csv.reader instance parsing estimate tables.
        :param _estimate_handler: file handler for estimate file.
        :param _estimate_path: path to estimate file, generated based on user provided path and sequence value.
        :param _geography_path: path to geography file, generated based on user provided path.
        :param _geography_records: populated by parsing a geography file.
            a dictionary populated with keys representing LOGRECNO values
            for a given row, and values representing the first six fields
            of that row.
        """
        self.conn_info = conn_info
        # If no delimiter given in config, default to ','
        self.delimiter = conn_info.get('delimiter', ',')
        self._estimate_reader = None
        self._estimate_handler = None
        self._estimate_path = None
        self._geography_path = None
        self._geography_records = {}

    def _get_paths(self):
        """
        Build paths for the geography and estimate files.
        Geography files begin a 'g' and have a CSV extension.
        Estimate files are based on the sequence number.
        """
        dir_contents = os.listdir(self.conn_info['path'])
        sequence_num = int(self.conn_info["sequence"])
        for f in dir_contents:
            # Get geography path. Slice doesn't need to be trapped for index error due to string < 3 char.
            if f[0] == 'g' and f[len(f)-3:] == 'csv':
                self._geography_path = os.path.join(self.conn_info['path'], f)
            # Set estimate path. Pass over when string is too short (index error) or
            # when characters f[8:12] are not coercible to integers (Value Error).
            if f[0] == 'e':
                try:
                    if int(f[8:12]) == sequence_num:
                        self._estimate_path = os.path.join(self.conn_info['path'], f)
                except (ValueError, IndexError):
                    pass

        # Raise Errors if not populated.
        if not self._geography_path:
            raise IOError("Expected geography file not found. Starts with 'g' and csv extent")
        if not self._estimate_path:
            raise IOError("Expected estimate file not found. Sequence given: %s." % self.conn_info["sequence"])

    def _build_logrecno_dict(self):
        """
        Create a dictionary of LOGRECNO values with associated attributes.
        Will be used as a lookup during iteration of estimate table.
        """
        geography_file_handle = open(self._geography_path, 'rt')
        geography_field_names = ['FILEID', 'STUSAB', 'SUMLEVEL', 'COMPONENT', 'LOGRECNO']
        geography_reader = csv.DictReader(geography_file_handle, geography_field_names, delimiter=self.delimiter)

        for record in geography_reader:
            self._geography_records[record['LOGRECNO']] = {
                'COMPONENT': record['COMPONENT'],
                'FILEID': record['FILEID'],
                'LOGRECNO': record['LOGRECNO'],
                'STUSAB': record['STUSAB'],
                'SUMLEVEL': record['SUMLEVEL']
            }

    def _build_estimate_reader(self):
        """
        Create a CSV reader using the estimate file.
        Reformat starting_position and individual field indexes
        for use in list lookup.
        """
        self._estimate_handler = open(self._estimate_path, 'rt')
        self._estimate_reader = csv.reader(self._estimate_handler, delimiter=self.delimiter)

        # Reformat the user-provided field indexes with values reflecting starting_position
        # Decrement by two to account for both the user-provided starting position and the field values
        start_index = int(self.conn_info['starting_position']) - 2
        for k, v in self.conn_info['fields'].iteritems():
            self.conn_info['fields'][k] = start_index + int(v)

    def __enter__(self):
        """
        Call internal setup functions.
        """
        self._get_paths()
        self._build_logrecno_dict()
        self._build_estimate_reader()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # http://www.itmaybeahack.com/book/python-2.6/html/p03/p03c07_contexts.html
        # Close the file handler.
        self._estimate_handler.close()
        if exc_type is not None:
            # Exception occurred
            return False  # Will raise the exception
        return True  # Everything's okay

    def __iter__(self):
        """
        Generator returning a dict of field name: field value pairs for each record.
        Combines estimate row with corresponding geography row based on common LOGRECNO value.
        """
        fields = self.conn_info['fields']
        for row in self._estimate_reader:
            logrecno = row[5]
            estimate_vals = {k: row[v] for k, v in fields.items()}
            # get the corresponding geographic record
            if logrecno in self._geography_records:
                # yield a concatenated estimate and geography dictionary
                geography_vals = self._geography_records[logrecno]
                yield dict(estimate_vals.items() + geography_vals.items())
            else:
                raise KeyError("LOGRECNO: %s not found in geography table." % str(logrecno))
