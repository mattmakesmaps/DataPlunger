# -*- coding: utf-8 -*-
__author__ = 'matt'
"""
Testing some basic unicode functionality.
"""

def text_notes(in_str):
    print type(in_str)
    print in_str
    print '----------------------------'

if __name__ == '__main__':
    # this is a python string that contains real
    # representations of utf8 characters.
    print "Python string containing utf8 characters."
    str= """
    На берегу пустынных волн
    Стоял он, дум великих полн,
    """

    text_notes(str)

    # decode() returns a Unicode object.
    print "decode() returns a Unicode object."
    str_utf8_decode = str.decode('utf8')
    text_notes(str_utf8_decode)

    ## raises UnicodeEncodeError
    #str_ascii_encode = str.encode('ascii')

    ## raises UnicodeEncodeError
    #print "Given a unicode object, return a python 8-bit string, ascii encoded"
    #str_utf8_decode_encode_as_ascii = str_utf8_decode.encode('ascii')
    #text_notes(str_utf8_decode_encode_as_ascii)

    print "Given a unicode object, return a python 8-bit string, utf8 encoded"
    str_utf8_decode_encode_as_utf8 = str_utf8_decode.encode('utf8')
    text_notes(str_utf8_decode_encode_as_utf8)


