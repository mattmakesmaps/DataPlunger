__author__ = 'mkenny'
import csv

class CSVReader(object):
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
