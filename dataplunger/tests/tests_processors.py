#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'matt'
__date__ = '3/2/14'
from dataplunger.processors import *
import os
import tempfile
from nose.tools import raises


class RecordConstructorMock(object):
    def __init__(self, **kwargs):
        self.records = []


class TestBase(object):
    def __init__(self):
        self.records = [{'name': u'Matt', 'age': 27, 'gender': u'male'}]
        self.devnull = ProcessorDevNull()


class TestProcessorCSVWriter(TestBase):
    """
    CSV writer should output a set of input records as a CSV file.
    """

    def setup(self):
        """
        Create a test file to store CSV output.
        """
        self.test_file = tempfile.mkstemp()

    def test_csvwriter(self):
        """
        Write records as CSV to a temporary output file.
        CSV should match expected values.
        """
        csv_writer = ProcessorCSVWriter(self.devnull, self.test_file[1], ['name', 'age', 'gender'])
        csv_writer.process(self.records)
        # Need to manually delete csv_writer, calling it's __exit__() method.
        # This closes the output file handle, allowing us to assert its contents.
        # I think that this happens normally when the processor is running as
        # part of a larger processing chain.
        del csv_writer
        # Check the output file.
        # test_file returns a tuple, with the path as the second element.
        expected = ['name,age,gender\r\n', 'Matt,27,male\r\n']
        with open(self.test_file[1], 'r') as test_file_handle:
            contents = test_file_handle.readlines()
            assert contents == expected

    def teardown(self):
        """
        Delete temp file if it still exists.
        """
        if os.path.isfile(self.test_file[1]):
            os.remove(self.test_file[1])


class TestProcessorDevNull(object):
    """
    ProcessorDevNull should populate its record_constructor's records attribute
    with the records that were passed into it. This resembles the end result of
    a processing chain's execution.
    """
    def __init__(self):
        self.records = [{'name': u'Matt', 'age': 27, 'gender': u'male'}]

    def test_devnull(self):
        """
        Assert ProcessorDevNull resets its record constructor with
        processed records.
        """
        p = ProcessorDevNull()
        records = p.process(self.records)
        assert records == self.records


class TestProcessorSortRecords(object):
    """
    Test ProcessorSortRecords. Currently, only an ascending sort
    on strings is implemented.
    """
    def __init__(self):
        self.records = [{'name': u'Bob', 'age': 30, 'gender': u'male'},
                        {'name': u'Matt', 'age': 27, 'gender': u'male'},
                        {'name': u'Luke', 'age': 31, 'gender': u'male'}]
        self.devnull = ProcessorDevNull()

    def test_sort_by_unicode_string(self):
        """
        Sort based on name field. Should return a list of records in the order of:
        'Bob', 'Luke', 'Matt'
        """
        expected_list = [
            {'name': u'Bob', 'age': 30, 'gender': u'male'},
            {'name': u'Luke', 'age': 31, 'gender': u'male'},
            {'name': u'Matt', 'age': 27, 'gender': u'male'}
        ]
        sort_processor = ProcessorSortRecords(self.devnull, sort_key='name')
        assert sort_processor.process(self.records) == expected_list

    def test_sort_by_int(self):
        """
        Sort based on age field. Should return a list of records in the order of:
        'Matt', 'Bob', 'Luke'
        """
        expected_list = [
            {'name': u'Matt', 'age': 27, 'gender': u'male'},
            {'name': u'Bob', 'age': 30, 'gender': u'male'},
            {'name': u'Luke', 'age': 31, 'gender': u'male'}
        ]
        sort_processor = ProcessorSortRecords(self.devnull, sort_key='age')
        assert sort_processor.process(self.records) == expected_list


class TestProcessorConcatenateFields(object):
    """
    Test ProcessorConcatenateFields.
    """
    def __init__(self):
        self.records = [{'name': u'Bob', 'int1': 30, 'int2': 10, 'gender': u'male'},
                        {'name': u'Matt', 'int1': 27, 'int2': 10, 'gender': u'male'},
                        {'name': u'Luke', 'int1': 31, 'int2': 10, 'gender': u'male'}]
        self.out_field = 'concat'

    def test_concatenate_text(self):
        """
        Create a new concatenated text field.
        """
        expected_list = [
            {'name': u'Bob', 'int1': 30, 'int2': 10, 'gender': u'male', 'concat': 'Bobmale'},
            {'name': u'Matt', 'int1': 27, 'int2': 10, 'gender': u'male', 'concat': 'Mattmale'},
            {'name': u'Luke', 'int1': 31, 'int2': 10, 'gender': u'male', 'concat': 'Lukemale'}
        ]

        p = ProcessorConcatenateFields(None, ['name', 'gender'], self.out_field)
        iter = p.process(self.records)
        index = 0
        for actual_record in iter:
            assert actual_record == expected_list[index]
            index += 1

    def test_concatenate_int(self):
        """
        Create a new concatenated integer field.
        """
        expected_list = [
            {'name': u'Bob', 'int1': 30, 'int2': 10, 'gender': u'male', 'concat': 40},
            {'name': u'Matt', 'int1': 27, 'int2': 10, 'gender': u'male', 'concat': 37},
            {'name': u'Luke', 'int1': 31, 'int2': 10, 'gender': u'male', 'concat': 41}
        ]

        p = ProcessorConcatenateFields(None, ['int1', 'int2'], self.out_field)
        iter = p.process(self.records)
        index = 0
        for actual_record in iter:
            assert actual_record == expected_list[index]
            index += 1


class TestProcessorGetData(TestBase):
    """
    Test ProcessorGetData.
    kwarg 'fields', a list of field names, is used to truncate an input record.
    """
    def test_get(self):
        pass


class TestProcessorCombineData_ValueHash(TestBase):
    """
    Test ProcessorCombineData_ValueHash.
    Requires two readers as input.
    """
    def __init__(self):
        """
        Create required configuration parameters.
        """
        self.join_keys = ['name']
        self.combine_reader_name = 'Test_Grades'
        self.readers = {
            'Test_Grades': {
                'path': os.path.join(os.path.dirname(__file__), 'test_data/grades.csv'),
                'type': 'ReaderCSV',
                'delimeter': ',',
            },
            'Test_People': {
                'path': os.path.join(os.path.dirname(__file__), 'test_data/people.csv'),
                'type': 'ReaderCSV',
                'delimeter': ',',
            }
        }

    def test_processorcombinedata_valuehash(self):
        """
        Test ProcessorCombineData_ValueHash Happy Path (Left Join)
        """
        # Build an existing reader to pass into the ProcessorCombineData_ValueHash instance.
        existing_reader_vals = [
            {'gender': u'male', 'age': 27, 'name': u'Matt'},
            {'gender': u'female', 'age': 27, 'name': u'Riley'},
            {'gender': u'male', 'age': 29, 'name': u'Steve'},
            {'gender': u'male', 'age': 40, 'name': u'Scott'}
        ]

        expected = [
            {'grade': u'A', 'gender': u'male', 'age': 27, 'name': u'Matt', 'subject': u'History'},
            {'grade': u'B', 'gender': u'male', 'age': 27, 'name': u'Matt', 'subject': u'Drama'},
            {'grade': u'C', 'gender': u'male', 'age': 27, 'name': u'Matt', 'subject': u'English'},
            {'grade': u'A', 'gender': u'female', 'age': 27, 'name': u'Riley', 'subject': u'History'},
            {'grade': u'B', 'gender': u'female', 'age': 27, 'name': u'Riley', 'subject': u'Drama'},
            {'grade': u'C', 'gender': u'female', 'age': 27, 'name': u'Riley', 'subject': u'Economics'},
            {'grade': u'', 'gender': u'male', 'age': 29, 'name': u'Steve', 'subject': u''},
            {'grade': u'A', 'gender': u'male', 'age': 40, 'name': u'Scott', 'subject': u'History'},
            {'grade': u'B', 'gender': u'male', 'age': 40, 'name': u'Scott', 'subject': u'Algebra'},
            {'grade': u'C', 'gender': u'male', 'age': 40, 'name': u'Scott', 'subject': u'English'}
        ]

        test_combine_inst_kwargs = {
           'processor': None,
           'reader': self.combine_reader_name,
           'keys': self.join_keys,
           'readers': self.readers
        }
        # Build inst using kwargs
        p = ProcessorCombineData_ValueHash(**test_combine_inst_kwargs)
        iter = p.process(existing_reader_vals)
        output = [r for r in iter]
        assert output == expected


class TestProcessorMatchValue(TestBase):
    """
    Test different combinations of ProcessorMatchValue.
    ProcessorMatchValue.process() should continue processing if:
      - A positive match is found, and the action is 'keep'.
      - A positive match is not found, and the action is 'discard'
    Processing should not continue if:
      - A positive match is found, and the action is 'discard'.
      - A positive match is not found, and the action is 'keep'.
    """
    def test_positivematch_actionkeep(self):
        """
        Assert record processing continues.
        """
        p = ProcessorMatchValue(None, matches={'name': 'Matt'}, action='keep')
        iter = p.process(self.records)
        expected = self.records[0]
        for record in iter:
            assert record == expected

    def test_multiple_positivematch_actionkeep(self):
        """
        Assert record processing continues.
        """
        p = ProcessorMatchValue(None, matches={'name': ['Matt', 'Greg', 5]}, action='keep')
        iter = p.process(self.records)
        expected = self.records[0]
        for record in iter:
            assert record == expected

    def test_positivematch_actiondiscard(self):
        """
        Assert record was removed from processing.
        """
        p = ProcessorMatchValue(None, matches={'name': 'Matt'}, action='discard')
        iter = p.process(self.records)
        for record in iter:
            assert record is None

    def test_multiple_positivematch_actiondiscard(self):
        """
        Assert record was removed from processing.
        """
        p = ProcessorMatchValue(None, matches={'name': ['Matt', 'Greg', 5]}, action='discard')
        iter = p.process(self.records)
        for record in iter:
            assert record is None

    def test_negativematch_actionkeep(self):
        """
        Assert record was removed from processing.
        """
        p = ProcessorMatchValue(None, matches={'name': 'Greg'}, action='keep')
        iter = p.process(self.records)
        for record in iter:
            assert record is None

    def test_multiple_negativematch_actionkeep(self):
        """
        Assert record was removed from processing.
        """
        p = ProcessorMatchValue(None, matches={'name': ['Greg', 5, 'Jane']}, action='keep')
        iter = p.process(self.records)
        for record in iter:
            assert record is None

    def test_negativematch_actiondiscard(self):
        """
        Assert record processing continues.
        """
        p = ProcessorMatchValue(None, matches={'name': 'Greg'}, action='discard')
        iter = p.process(self.records)
        expected = self.records[0]
        for record in iter:
            assert record == expected

    def test_multiple_negativematch_actiondiscard(self):
        """
        Assert record processing continues.
        """
        p = ProcessorMatchValue(None, matches={'name': ['Greg', 5, 'Jane']}, action='discard')
        iter = p.process(self.records)
        expected = self.records[0]
        for record in iter:
            assert record == expected


class TestProcessorChangeCase(TestBase):
    """
    Test ProcessorChangeCase.
    kwarg 'case' controls output case of dictionary values for
    a given input record.

    case = 'Upper' - call upper() method on string.
    case = 'Lower' - call lower() method on string.
    """
    def test_upper(self):
        """
        Assert values are converted to upper-case.
        """
        p = ProcessorChangeCase(None, case='Upper')
        expected = {'name': u'MATT', 'age': 27, 'gender': u'MALE'}
        iter = p.process(self.records)
        for record in iter:
            assert record == expected

    def test_lower(self):
        """
        Assert values are converted to lower-case.
        """
        p = ProcessorChangeCase(None, case='Lower')
        expected = {'name': u'matt', 'age': 27, 'gender': u'male'}
        iter = p.process(self.records)
        for record in iter:
            assert record == expected

    @raises(ValueError)
    def test_bad_case(self):
        """
        A bad case should raise a ValueError.
        """
        p = ProcessorChangeCase(None, case='Blerch')
        iter = p.process(self.records)
        for record in iter:
            pass


class TestProcessorTruncateFields(TestBase):
    """
    Test ProcessorTruncateFields.
    kwarg 'fields', a list of field names, is used to truncate an input record.
    """
    def test_truncate(self):
        """
        Assert record is truncated to name and age fields only.
        """
        p = ProcessorTruncateFields(None, fields=['name', 'age'])
        expected = {'name': u'Matt', 'age': 27}
        iter = p.process(self.records)
        for record in iter:
            assert record == expected
