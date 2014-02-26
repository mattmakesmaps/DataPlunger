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

    self.conn_info - the connection information (pathway) for a given file.
    self._file_handler - set in __enter__(), a read only pointer to the CSV.
    self._dict_reader - an instance of csv.dict_reader()
    """
    def __init__(self, conn_info):
        self.conn_info = conn_info
        self._file_handler = None
        self._dict_reader = None

    def __enter__(self):
        """
        Open a file connection, pass that to an instance of csv.DictReader
        """
        self._file_handler = open(self.conn_info['path'], 'rt')
        self._dict_reader = csv.DictReader(self._file_handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # http://www.itmaybeahack.com/book/python-2.6/html/p03/p03c07_contexts.html
        # Close the file handler.
        self._file_handler.close()
        if exc_type is not None:
            # Exception occurred
            return False # Will raise the exception
        return True # Everything's okay

    def __iter__(self):
        # Generator returning a dict of field name: field value pairs for each record.
        for row in self._dict_reader:
            yield row


class ReaderCensus(ReaderBaseClass):
    """
    Reader class implementation for Census ACS Summary Files.

    self.conn_info.path - contains a pathway to an ACS formatted directory.
        This directory contains estimates and geography tables.
    self.conn_info.sequence - the sequence number of interest as defined
        in 'Sequence_Number_and_Table_Number_Lookup.xls' of the Census.

    TODO: Reader should parse the geometry table first, creating a lookup table
    of LOGRECNO values that can mapped by the generator to each record from
    the estimate table.

    Could be composed of two CSV readers for parsing geography/estimate CSVs.
    """
    def __init__(self, conn_info):
        self.conn_info = conn_info
        self._file_handler = None
        self._estimate_dict_reader = None
        self._estimate_path = None
        self._geography_path = None
        self._geography_records = {}

    def _get_paths(self):
        """
        Given a directory, build paths for the geography and estimate files.
        Geography files begin a 'g' and have a CSV extension.
        Estimate files are based on the sequence number
        """
        dir_contents = os.listdir(self.conn_info['path'])
        sequence_num = int(self.conn_info["sequence"])
        for f in dir_contents:
            if f[0] == 'g' and f[len(f)-3:] == 'csv':
                self._geography_path = os.path.join(conn_info['path'], f)
            # TODO: trap for instance in which file length <12 char.
            if f[0] == 'e' and int(f[8:12]) == sequence_num:
                self._estimate_path = os.path.join(conn_info['path'], f)

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
        dReader = csv.DictReader(geography_file_handle, geography_field_names)

        for record in dReader:
            self._geography_records[record['LOGRECNO']] = {
                'COMPONENT': record['COMPONENT'],
                'FILEID': record['FILEID'],
                'LOGRECNO': record['LOGRECNO'],
                'STUSAB': record['STUSAB'],
                'SUMLEVEL': record['SUMLEVEL']
            }

    def _build_estimate_reader(self):
        """
        TODO: Create a CSV reader that contains the six
        expected fields within the estimates file, along with
        the n-number of named fields provided by the user in the
        conn_info configuration block.
        """
        self._file_handler = open(self._estimate_path, 'rt')
        self._estimate_dict_reader = csv.DictReader(self._file_handler)

    def __enter__(self):
        """
        Open a file connection, pass that to an instance of csv.DictReader
        """
        # populate paths
        self._get_paths()
        self._build_logrecno_dict()
        self._build_estimate_reader()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # http://www.itmaybeahack.com/book/python-2.6/html/p03/p03c07_contexts.html
        # Close the file handler.
        self._file_handler.close()
        if exc_type is not None:
            # Exception occurred
            return False # Will raise the exception
        return True # Everything's okay

    def __iter__(self):
        # Generator returning a dict of field name: field value pairs for each record.
        for row in self._dict_reader:
            yield row

if __name__ == '__main__':
    conn_info = {'path': '/Users/matt/Projects/dataplunger/sample_data/Washington_All_Geographies_Tracts_Block_Groups_Only', 'sequence': 2}
    mR = ReaderCensus(conn_info)
    mR._get_paths()
    mR._build_logrecno_dict()
    print "done"
