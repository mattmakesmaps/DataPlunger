__author__ = 'matt'
__date__ = '3/27/14'
"""
Test for Reader class implementations.
"""
from nose.tools import raises
from dataplunger.readers import *


class TestReaderCensus(object):
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
