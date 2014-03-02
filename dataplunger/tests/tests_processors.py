__author__ = 'matt'
__date__ = '3/2/14'
from dataplunger.processors import *
from nose.tools import raises


class RecordConstructorMock(object):
    def __init__(self):
        self.records = []


class TestBase(object):
    def __init__(self):
        self.record = {'name': 'Matt', 'age': '27', 'gender': 'male'}
        self.record_constructor = RecordConstructorMock()
        self.devnull = ProcessorDevNull(self.record_constructor)


class TestProcessorMatchValue(TestBase):
    """
    Test different combinations of ProcessorMatchValue.
    ProcessorMatchValue.process() should continue processing if:
      - A positive match is found, and the action is 'keep'.
      - A positive match is not found, and the action is 'discard'
    Processing should not continue if:
      - A positive match is found, and the action is 'disard'.
      - A positive match is not found, and the action is 'keep'.
    """
    def test_positivematch_actionkeep(self):
        p = ProcessorMatchValue(self.devnull, matches={'name': 'Matt'}, action='keep')
        assert p.process(self.record) == self.record

    def test_positivematch_actiondiscard(self):
        p = ProcessorMatchValue(self.devnull, matches={'name': 'Matt'}, action='discard')
        assert p.process(self.record) is None

    def test_negativematch_actionkeep(self):
        p = ProcessorMatchValue(self.devnull, matches={'name': 'Greg'}, action='keep')
        assert p.process(self.record) is None

    def test_negativematch_actiondiscard(self):
        p = ProcessorMatchValue(self.devnull, matches={'name': 'Greg'}, action='discard')
        assert p.process(self.record) == self.record


class TestProcessorChangeCase(TestBase):
    """
    Test ProcessorChangeCase.
    kwarg 'case' controls output case of dictionary values for
    a given input record.

    case = 'Upper' - call upper() method on string.
    case = 'Lower' - call lower() method on string.
    """
    def test_upper(self):
        p = ProcessorChangeCase(self.devnull, case='Upper')
        expected = {'name': 'MATT', 'age': '27', 'gender': 'MALE'}
        assert p.process(self.record) == expected

    def test_lower(self):
        p = ProcessorChangeCase(self.devnull, case='Lower')
        expected = {'name': 'matt', 'age': '27', 'gender': 'male'}
        assert p.process(self.record) == expected


class TestProcessorTruncateFields(TestBase):
    """
    Test ProcessorTruncateFields.
    krwarg 'fields', a list of field names, is used to truncate an input record.
    """
    def test_truncate(self):
        p = ProcessorTruncateFields(self.devnull, fields=['name', 'age'])
        expected = {'name': 'Matt', 'age': '27'}
        assert p.process(self.record) == expected
