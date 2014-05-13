__author__ = 'matt'
__date__ = '5/8/14'
import heapq
import pprint
import cPickle
import os

def make_sorted_list(records, sort_key):
    """Return a list of dicts, sorted by key of interest."""
    print "Individual Sorted List"
    sorted_out = sorted(records, key=lambda k: k[sort_key])
    tuple_convert = [(r[sort_key], r) for r in sorted_out]
    pprint.pprint(tuple_convert)
    return tuple_convert

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

    """
    # From Source, create three sorted lists
    elem1 = make_sorted_list(source[0:3], 'name')
    elem2 = make_sorted_list(source[3:6], 'name')
    elem3 = make_sorted_list(source[6:9], 'name')

    heap_iter = heapq.merge(elem1, elem2, elem3)

    print "Final Sorted Output"
    pprint.pprint([r for r in heap_iter])
    """

    # create iter for source
    buff_size = 3
    source_iter = iter(source)

    sorted_iterables = []

    processing = True
    while processing:
        records_to_sort = []
        for i in range(buff_size):
            try:
                records_to_sort.append(i)
            except StopIteration:
                processing = False
        sorted_records = make_sorted_list(records_to_sort, 'name')
        sorted_iterables.append(sorted_records)


    """
    with open(pathway, 'w+b') as p_handle:
        print "dumping"
        v1 = cPickle.dumps('hello')
        v2 = cPickle.dumps(['1',2,'three'])
        v3 = cPickle.dumps({'key':'value'})
        p_handle.writelines([v1,v2,v3])

    with open(pathway, 'r+b') as p_handle2:
        print "loading"
        for i in p_handle2:
            try:
                print cPickle.loads(i)
            except EOFError:
                break
    """