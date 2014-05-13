__author__ = 'matt'
__date__ = '5/12/14'

import cPickle, os, pprint

class PickleParty(object):
    """
    - Pickle contents to a file.
    - Implements an __iter__() method that yields a deserialized record.
    - Should also implement a counter that raises a StopIteration error
      to avoid an EOF error.
    """

    def __init__(self, file_path):
        self.count = 0
        self.file_handle = open(file_path, 'w+b')
        self._pickler = cPickle.Pickler(self.file_handle, 2)
        self._unpickler = cPickle.Unpickler(self.file_handle)

    def dump(self, val):
        """Serialize contents to a file."""
        self._pickler.dump(val)
        self.count += 1
        return True

    def load(self):
        """Deserialize contents."""
        record = self._unpickler.load()
        self.count -= 1
        return record

    def flush(self):
        """Flush Contents to File."""
        self.file_handle.flush()
        return True

    def seek(self, position):
        """Seek File"""
        self.file_handle.seek(position)
        return True

    def __del__(self, exc_type=None, exc_val=None, exc_tb=None):
        """Close the db connection. Note: Will be Called Twice if a Context Manager is used."""
        if self.file_handle:
            self.file_handle.close()
        if exc_type is not None:
            # Exception occurred
            return False  # Will raise the exception
        return True  # Everything's okay

    def __iter__(self):
        """
        yield a deserialized record from the pickled file.
        """
        while self.count > 0:
            yield self.load()
        else:
            raise StopIteration

if __name__ == '__main__':
    source = [
        {'name': 'Matt', 'hometown': 'Oxnard', 'age': 27},
        {'name': 'Andrew', 'hometown': 'Menlo Park', 'age': 33},
        {'name': 'Doug', 'hometown': 'London', 'age': 12},
        {'name': 'Vanya', 'hometown': 'Fremont', 'age': 7},
        {'name': 'Stacie', 'hometown': 'Aberdeen', 'age': 98},
        {'name': 'Jim', 'hometown': 'Irwindale', 'age': 54},
        {'name': 'Pat', 'hometown': 'Oxnard', 'age': 23},
        {'name': 'Pat', 'hometown': 'Ventura', 'age': 13},
        {'name': 'Adam', 'hometown': 'Santa Paula', 'age': 55}
    ]
    out_path = os.path.join(os.path.dirname(__file__), 'temp/p_test.pkl')
    print out_path
    # Create instance.
    p_handle = PickleParty(out_path)
    # Populate with data
    for r in source:
        p_handle.dump(r)

    # Flush/Seek(0)
    p_handle.flush()
    p_handle.seek(0)

    values = [r for r in p_handle]
    pprint.pprint(values)
