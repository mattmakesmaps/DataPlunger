#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
    Test class for the Census Reader.

    Test data come from the US Census. Relevant Links:

    * `Lookup table`_ for Sequence Numbers, Line Numbers, Starting Positions.
    * Test data for `Washington State`_.

    .. _Lookup table: http://www2.census.gov/acs2012_5yr/summaryfile/Sequence_Number_and_Table_Number_Lookup.xls
    .. _http://www2.census.gov/acs2012_5yr/summaryfile/2008-2012_ACSSF_By_State_All_Tables/Washington_All_Geographies_Tracts_Block_Groups_Only.zip
    """
    def __init__(self):
        """
        Create connection info
        """
        self.kwargs = {'starting_position': 87,
                          'sequence': 2,
                          'fields': {'Total': 1, 'Female': 17, 'Male': 2},
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
        expected = {'Total': 14,
                    'COMPONENT': '00',
                    'STUSAB': 'WA',
                    'SUMLEVEL': '140',
                    'Female': 2,
                    'LOGRECNO': '0004357',
                    'Male': 12,
                    'FILEID': 'ACSSF'}
        with ReaderCensus(**self.kwargs) as t_reader:
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
        self.kwargs = {'starting_position': 87,
                          'sequence': 2,
                          'fields': {'Total': 1, 'Female': 17, 'Male': 2},
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
        self.kwargs['path'] = tempfile.mkdtemp()
        # Create mocks for geography and estimate files
        self.geog_file_mock = open(os.path.join(self.kwargs['path'], 'g20125wa.csv'), 'w')
        self.estimate_file_mock = open(os.path.join(self.kwargs['path'], 'e20125wa0002000.txt'), 'w')
        self.estimate_index_error_mock = open(os.path.join(self.kwargs['path'], 'eshort.txt'), 'w')
        self.estimate_value_error_mock = open(os.path.join(self.kwargs['path'], 'election_2012_kc.csv'), 'w')

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
        with ReaderCensus(**self.kwargs) as t_reader:
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
        with ReaderCensus(**self.kwargs) as t_reader:
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
        with ReaderCensus(**self.kwargs) as t_reader:
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
        with ReaderCensus(**self.kwargs) as t_reader:
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

        if os.path.isdir(self.kwargs['path']):
            os.rmdir(self.kwargs['path'])


class TestReaderCSV(object):
    """
    Test class for the csv reader.
    """
    def __init__(self):
        """
        Note the mixing of potential values for self.field_types.
        """
        self.path = os.path.join(os.path.dirname(__file__), "test_data/election_2010_kc.csv")
        self.delimiter = ","
        self.field_types = {
            'SumOfCount': 'int',
            'Candidate': 'string',
            'Legislative District': 'integer',
            'County Council District': 'integer',
            'Race': 'unicode',
            'Party': 'text',
            'Precinct': 'string',
            'Congressional District': 'int'
        }

    def test_readercsv_with_mapping(self):
        """
        Test ReaderCSV behavior with field mapping provided.
        Integers should be cast to int, strings to str.
        """
        expected = {'SumOfCount': 212,
                    'Candidate': u'APPROVED',
                    'Legislative District': 47,
                    'County Council District': 9,
                    'Race': u'Amendment to the State Constitution Engrossed Substitute House Joint Resolution No. 4220',
                    'Party': u'NP',
                    'Precinct': u'KELLY',
                    'Congressional District': 8}
        with ReaderCSV(self.path, self.delimiter, self.field_types) as t_reader:
            for record in t_reader:
                assert record == expected
                break

    def test_readercsv_no_mapping(self):
        """
        Test ReaderCSV behavior with no field mapping provided.
        All outputs should be strings.
        """
        expected = {'SumOfCount': u'212',
                    'Candidate': u'APPROVED',
                    'Legislative District': u'47',
                    'County Council District': u'9',
                    'Race': u'Amendment to the State Constitution Engrossed Substitute House Joint Resolution No. 4220',
                    'Party': u'NP',
                    'Precinct': u'KELLY',
                    'Congressional District': u'8'}
        with ReaderCSV(self.path, self.delimiter) as t_reader:
            for record in t_reader:
                assert record == expected
                break

class TestReaderSHP(object):
    """
    Test class for the SHP reader.
    """

    def __init__(self):
        """Create connection info"""
        self.path = os.path.join(os.path.dirname(__file__), "test_data/50m_lakes_utf8.shp")

    def test_readershp(self):
        """
        Given a connection to a CSV file.
        Open a connection via an instance of the ReaderSHP class
        One iteration should yield the expected record.

        NOTE: Fiona type isn't parsed from the SHP DBF, as such, Fiona doesn't output
          an Unicode object.
        """
        expected = {u'scalerank': 2,
                    u'admin': None,
                    u'name': u'Mälaren',
                    u'note': None,
                    'geometry': {'type': 'Polygon', 'coordinates': [[(17.979785156250017, 59.329052734375), (17.87617187500001, 59.27080078125), (17.57050781250001, 59.267626953125), (17.474511718750023, 59.29150390625), (17.370703125000006, 59.294921875), (17.304589843750023, 59.27216796875), (17.175195312500023, 59.355810546875), (17.06562500000001, 59.3732421875), (16.913867187500017, 59.445849609375), (16.742285156250006, 59.430615234375), (16.610449218750006, 59.453515624999994), (16.144335937500017, 59.44775390625), (16.044238281250017, 59.478466796875), (16.251757812500017, 59.493212890625), (16.47265625, 59.519384765625), (16.573828125000006, 59.611669921875), (16.646875000000023, 59.55927734375), (16.752343750000023, 59.543310546875), (16.84101562500001, 59.5875), (16.9775390625, 59.550683593749994), (17.06269531250001, 59.569238281249994), (17.3720703125, 59.495751953124994), (17.390527343750023, 59.58447265625), (17.534472656250017, 59.539404296875), (17.687304687500017, 59.5416015625), (17.67158203125001, 59.594775390625), (17.760058593750017, 59.620507812499994), (17.785937500000017, 59.597998046875), (17.80859375, 59.55322265625), (17.772851562500023, 59.414111328125), (17.82929687500001, 59.37900390625), (17.964257812500023, 59.359375), (17.979785156250017, 59.329052734375)]]},
                    u'featurecla': u'Lake',
                    'fiona_id': '0',
                    'fiona_type': 'Feature',
                    u'name_alt': None}
        with ReaderSHP(self.path) as t_reader:
            for record in t_reader:
                assert record == expected
                break
