#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'mkenny'
__date__ = '5/12/14'
"""
OUTPUT
------
Individual Sorted List
[('Andrew', {'age': 33, 'hometown': 'Menlo Park', 'name': 'Andrew'}),
 ('Doug', {'age': 12, 'hometown': 'London', 'name': 'Doug'}),
 ('Matt', {'age': 27, 'hometown': 'Oxnard', 'name': 'Matt'})]
Individual Sorted List
[('Jim', {'age': 54, 'hometown': 'Irwindale', 'name': 'Jim'}),
 ('Stacie', {'age': 98, 'hometown': 'Aberdeen', 'name': 'Stacie'}),
 ('Vanya', {'age': 7, 'hometown': 'Fremont', 'name': 'Vanya'})]
Individual Sorted List
[('Adam', {'age': 55, 'hometown': 'Santa Paula', 'name': 'Adam'}),
 ('Pat', {'age': 23, 'hometown': 'Oxnard', 'name': 'Pat'}),
 ('Pat', {'age': 13, 'hometown': 'Ventura', 'name': 'Pat'})]
Final Sorted Output
[('Adam', {'age': 55, 'hometown': 'Santa Paula', 'name': 'Adam'}),
 ('Andrew', {'age': 33, 'hometown': 'Menlo Park', 'name': 'Andrew'}),
 ('Doug', {'age': 12, 'hometown': 'London', 'name': 'Doug'}),
 ('Jim', {'age': 54, 'hometown': 'Irwindale', 'name': 'Jim'}),
 ('Matt', {'age': 27, 'hometown': 'Oxnard', 'name': 'Matt'}),
 ('Pat', {'age': 23, 'hometown': 'Oxnard', 'name': 'Pat'}),
 ('Pat', {'age': 13, 'hometown': 'Ventura', 'name': 'Pat'}),
 ('Stacie', {'age': 98, 'hometown': 'Aberdeen', 'name': 'Stacie'}),
 ('Vanya', {'age': 7, 'hometown': 'Fremont', 'name': 'Vanya'})]
"""


import heapq
import pprint

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

    # From Source, create three sorted lists
    elem1 = make_sorted_list(source[0:3], 'name')
    elem2 = make_sorted_list(source[3:6], 'name')
    elem3 = make_sorted_list(source[6:9], 'name')

    heap_iter = heapq.merge(elem1, elem2, elem3)

    print "Final Sorted Output"
    pprint.pprint([r for r in heap_iter])
