__author__ = 'matt'
__date__ = '3/29/14'
import itertools

def truncate_fields(iterable):
    """
    Return a dictionary constrained to the given set of keys ,'out_fields'.
    """
    print "in truncate fields"
    out_fields = set(['name', 'gender'])
    truncated_record = {k: v for k, v in iterable.iteritems() if k in out_fields}
    return truncated_record

def change_case(inLine):
    print "in change case"

    case = 'upper'
    if case == 'upper':
        #inLine = {key: value.upper() for key, value in inLine.iteritems() if isinstance(value, str)}
        inLine.update((k, v.upper()) for k, v in inLine.items() if isinstance(v, str))
    elif case == 'lower':
        #inLine = {key: value.lower() for key, value in inLine.iteritems() if isinstance(value, str)}
        inLine.update((k, v.lower()) for k, v in inLine.items() if isinstance(v, str))
    else:
        raise ValueError("Case Not Supported")
    return inLine

if __name__ == '__main__':
    """
    Pass a list to truncate_iterator.
    Pass truncate iterator to change_case_iterator
    Pass change_case_iterator to sorted_records
    """
    data = [
        {'name': 'Matt', 'age': 27, 'gender':'male'},
        {'name': 'Jan', 'age': 32, 'gender':'female'},
        {'name': 'Stewart', 'age': 10, 'gender':'male'}
    ]

    truncate_iterator = itertools.imap(truncate_fields, data)
    change_case_iterator = itertools.imap(change_case, truncate_iterator)
    sorted_records = sorted(change_case_iterator, key=lambda k: k['name'])
    print sorted_records
