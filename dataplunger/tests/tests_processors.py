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
        self.records = [{'name': 'Matt', 'age': '27', 'gender': 'male'}]
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
        self.records = [{'name': 'Matt', 'age': '27', 'gender': 'male'}]

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
        self.records = [{'name': 'Matt', 'age': '27', 'gender': 'male'},
                        {'name': 'Bob', 'age': '30', 'gender': 'male'},
                        {'name': 'Luke', 'age': '31', 'gender': 'male'}]
        self.devnull = ProcessorDevNull()

    def test_sortrecords(self):
        """
        Sort based on name field. Should return records in the order of:
        'Bob', 'Luke', 'Matt'
        """
        expected_list = [
            {'name': 'Bob', 'age': '30', 'gender': 'male'},
            {'name': 'Luke', 'age': '31', 'gender': 'male'},
            {'name': 'Matt', 'age': '27', 'gender': 'male'}
        ]
        sort_processor = ProcessorSortRecords(self.devnull, sortby='name')
        assert sort_processor.process(self.records) == expected_list

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
        p = ProcessorMatchValue(self.devnull, matches={'name': 'Matt'}, action='keep')
        iter = p.process(self.records)
        expected = self.records[0]
        for record in iter:
            assert record == expected

    def test_positivematch_actiondiscard(self):
        """
        Assert record was removed from processing.
        """
        p = ProcessorMatchValue(self.devnull, matches={'name': 'Matt'}, action='discard')
        iter = p.process(self.records)
        for record in iter:
            assert record is None

    def test_negativematch_actionkeep(self):
        """
        Assert record was removed from processing.
        """
        p = ProcessorMatchValue(self.devnull, matches={'name': 'Greg'}, action='keep')
        iter = p.process(self.records)
        for record in iter:
            assert record is None

    def test_negativematch_actiondiscard(self):
        """
        Assert record processing continues.
        """
        p = ProcessorMatchValue(self.devnull, matches={'name': 'Greg'}, action='discard')
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
        p = ProcessorChangeCase(self.devnull, case='Upper')
        expected = {'name': 'MATT', 'age': '27', 'gender': 'MALE'}
        iter = p.process(self.records)
        for record in iter:
            assert record == expected


    def test_lower(self):
        """
        Assert values are converted to lower-case.
        """
        p = ProcessorChangeCase(self.devnull, case='Lower')
        expected = {'name': 'matt', 'age': '27', 'gender': 'male'}
        iter = p.process(self.records)
        for record in iter:
            assert record == expected

    @raises(ValueError)
    def test_bad_case(self):
        """
        A bad case should raise a ValueError.
        """
        p = ProcessorChangeCase(self.devnull, case='Blerch')
        iter = p.process(self.records)
        for record in iter:
            pass

class TestProcessorTruncateFields(TestBase):
    """
    Test ProcessorTruncateFields.
    krwarg 'fields', a list of field names, is used to truncate an input record.
    """
    def test_truncate(self):
        """
        Assert record is truncated to name and age fields only.
        """
        p = ProcessorTruncateFields(self.devnull, fields=['name', 'age'])
        expected = {'name': 'Matt', 'age': '27'}
        iter = p.process(self.records)
        for record in iter:
            assert record == expected
