__author__ = 'matt'
__date__ = '3/27/14'
"""
Test for Reader class implementations.
"""
from nose.tools import raises
from dataplunger.readers import *
import tempfile
import os


class TestReaderCensus_Success(object):
    """
    Test class for the census reader.
    """
    def __init__(self):
        """
        Create connection info
        """
        self.conn_info = {'starting_position': 87,
                          'sequence': 2,
                          'fields': {'Total': 1, 'Female': 17, 'Male': 2},
                          'encoding': 'UTF-8',
                          'delimiter': ',',
                          'path': os.path.join(os.path.dirname(__file__),
                                               'test_data/Washington_All_Geographies_Tracts_Block_Groups_Only'),
                          'type': 'ReaderCensus'}

    def test_readercensus(self):
        """
        Given a connection to a set of census files,
        Open a connection via an instance of the ReaderCensus class
        One iteration should yield the expected record.
        """
        expected = {'Total': '14',
                    'COMPONENT': '00',
                    'STUSAB': 'WA',
                    'SUMLEVEL': '140',
                    'Female': '2',
                    'LOGRECNO': '0004357',
                    'Male': '12',
                    'FILEID': 'ACSSF'}
        with ReaderCensus(self.conn_info) as t_reader:
            for record in t_reader:
                assert record == expected
                break


class TestReaderCensus_BadEstGeogFiles(object):
    """
    Test that an IOError is raised when folder in conn_info['path']
    does not contain a geography file in the expected naming format.
    """
    def __init__(self):
        """
        Create connection info
        """
        self.conn_info = {'starting_position': 87,
                          'sequence': 2,
                          'fields': {'Total': 1, 'Female': 17, 'Male': 2},
                          'encoding': 'UTF-8',
                          'delimiter': ',',
                          'type': 'ReaderCensus'}

    def _close_remove_file(self, file):
        """
        If given file exists, close it and remove it.
        """
        if os.path.isfile(file.name):
            if not file.closed:
                file.close()
            os.remove(file.name)

    def setup(self):
        """
        Create temporary directory and populate with files containing
        expected geography and estimate names.
        """
        self.conn_info['path'] = tempfile.mkdtemp()
        # Create mocks for geography and estimate files
        self.geog_file_mock = open(os.path.join(self.conn_info['path'], 'g20125wa.csv'), 'w')
        self.estimate_file_mock = open(os.path.join(self.conn_info['path'], 'e20125wa0002000.txt'), 'w')
        self.estimate_index_error_mock = open(os.path.join(self.conn_info['path'], 'eshort.txt'), 'w')
        self.estimate_value_error_mock = open(os.path.join(self.conn_info['path'], 'election_2012_kc.csv'), 'w')

    @raises(IOError)
    def test_nogeographyfile(self):
        """
        Given a connection to a set of census file mocks,
        remove the geography file and test that an IOError is raised.
        """
        # Delete Geography File and Estimate Error Files.
        for file in [self.geog_file_mock, self.estimate_index_error_mock, self.estimate_value_error_mock]:
            self._close_remove_file(file)

        # Execute ReaderCensus.__enter__() method.
        with ReaderCensus(self.conn_info) as t_reader:
            pass

    @raises(IOError)
    def test_noestimatefile(self):
        """
        Given a connection to a set of census file mocks,
        remove the estimate file and test that an IOError is raised.
        """
        # Delete Estimate File and Error mocks
        for file in [self.estimate_file_mock, self.estimate_index_error_mock, self.estimate_value_error_mock]:
            self._close_remove_file(file)

        # Execute ReaderCensus.__enter__() method.
        with ReaderCensus(self.conn_info) as t_reader:
            pass

    @raises(IOError)
    def test_estimate_index_error(self):
        """
        Given a connection to a set of census file mocks,
        Ensure that an Index Error is caught and passed to skip over files
        beginning with 'e' that are too short for slice operation.
        """
        # Delete Estimate File and value error mock
        for file in [self.estimate_file_mock, self.estimate_value_error_mock]:
            self._close_remove_file(file)

        # Execute ReaderCensus.__enter__() method.
        with ReaderCensus(self.conn_info) as t_reader:
            pass

    @raises(IOError)
    def test_estimate_index_error(self):
        """
        Given a connection to a set of census file mocks,
        Ensure that a Value Error is caught and passed to skip over files
        beginning with 'e' whoose characters 8:12 can't be converted to int.
        """
        # Delete Estimate File and Index Error Mock
        for file in [self.estimate_file_mock, self.estimate_index_error_mock]:
            self._close_remove_file(file)

        # Execute ReaderCensus.__enter__() method.
        with ReaderCensus(self.conn_info) as t_reader:
            pass

    def teardown(self):
        """
        Remove contents of temp directory and delete.
        """
        for file in [self.geog_file_mock,
                     self.estimate_file_mock,
                     self.estimate_index_error_mock,
                     self.estimate_value_error_mock]:
            self._close_remove_file(file)

        if os.path.isdir(self.conn_info['path']):
            os.rmdir(self.conn_info['path'])


class TestReaderCSV(object):
    """
    Test class for the csv reader.
    """
    def __init__(self):
        """
        Create connection info
        """
        self.conn_info = {
            "type": "ReaderCSV",
            "path": os.path.join(os.path.dirname(__file__), "test_data/election_2010_kc.csv"),
            "delimeter": ",",
            "encoding": "UTF-8"
        }

    def test_readercsv(self):
        """
        Given a connection to a CSV file.
        Open a connection via an instance of the ReaderCSV class
        One iteration should yield the expected record.
        """
        expected = {'SumOfCount': '212',
                    'Candidate': 'APPROVED',
                    'Legislative District': '47',
                    'County Council District': '9',
                    'Race': 'Amendment to the State Constitution Engrossed Substitute House Joint Resolution No. 4220',
                    'Party': 'NP',
                    'Precinct': 'KELLY',
                    'Congressional District': '8'}
        with ReaderCSV(self.conn_info) as t_reader:
            for record in t_reader:
                assert record == expected
                break
