# From: http://code.activestate.com/recipes/576755-sorting-big-files-the-python-26-way/
# based on Recipe 466302: Sorting big files the Python 2.4 way
# by Nicolas Lehuen

import os
from tempfile import gettempdir
from itertools import islice, cycle
from collections import namedtuple
import heapq

Keyed = namedtuple("Keyed", ["key", "obj"])

def merge(key=None, *iterables):
    # based on code posted by Scott David Daniels in c.l.p.
    # http://groups.google.com/group/comp.lang.python/msg/484f01f1ea3c832d

    if key is None:
        # heapq does the magic merge on each chunk.
        # see: https://docs.python.org/2/library/heapq.html
        for element in heapq.merge(*iterables):
            yield element
    else:
        keyed_iterables = [(Keyed(key(obj), obj) for obj in iterable)
                        for iterable in iterables]
        for element in heapq.merge(*keyed_iterables):
            yield element.obj

def batch_sort(input, output, key=None, buffer_size=32000, tempdirs=None):
    if tempdirs is None:
        tempdirs = []
    if not tempdirs:
        tempdirs.append(gettempdir())

    chunks = []
    try:
        with open(input,'rb',64*1024) as input_file:
            # Return an iterator for a given object.
            input_iterator = iter(input_file)
            # cycle() will just continue to loop through list of tempdirs.
            for tempdir in cycle(tempdirs):
                """
                islice() will return an iterator that behaves like slice().

                In this case, the object to be sliced will be the input_iterator,
                the number of elements (e.g. size of slice) is the buffer_size.

                list() calls the iterator returned by islice, populating an
                actual list.

                Since `input_iterator` is an actual iterator itself, the value
                for current_chunk always starts where the previous current_chunk
                instance left off.

                # Example of islice() instances referencing a list as input.
                # Note that both s1, and s2 return values 1 and 2.
                >>> from itertools import islice
                >>> ml = [1,2,3,4,5,6]
                >>> s1 = islice(ml,2)
                >>> list(s1)
                [1, 2]
                >>> s2 = islice(ml,2)
                >>> list(s2)
                [1, 2]
                # Creating an iterator from the list.
                # Note that s2 now returns 3, 4. since the iterator.
                # remembered its previous location.
                >>> i1 = iter(ml)
                >>> s1 = islice(i1, 2)
                >>> list(s1)
                [1, 2]
                >>> s2 = islice(i1, 2)
                >>> list(s2)
                [3, 4]
                """
                current_chunk = list(islice(input_iterator,buffer_size))
                # An empty list (from iterating through all records) breaks the loop.
                if not current_chunk:
                    break
                # sort the chunk
                current_chunk.sort(key=key)
                # open a handle to new output data file
                output_chunk = open(os.path.join(tempdir,'%06i'%len(chunks)),'w+b',64*1024)
                # Append the file handle to list chunks.
                chunks.append(output_chunk)
                # Write the chunk to output_chunk. Flush ensure that data
                # are actually written out to file.
                # Reset output_chunk file handle to start of file.
                output_chunk.writelines(current_chunk)
                output_chunk.flush()
                output_chunk.seek(0)
        with open(output,'wb',64*1024) as output_file:
            output_file.writelines(merge(key, *chunks))
    finally:
        for chunk in chunks:
            try:
                chunk.close()
                os.remove(chunk.name)
            except Exception:
                pass


if __name__ == '__main__':
    import optparse
    parser = optparse.OptionParser()
    parser.add_option(
        '-b','--buffer',
        dest='buffer_size',
        type='int',default=32000,
        help='''Size of the line buffer. The file to sort is
            divided into chunks of that many lines. Default : 32,000 lines.'''
    )
    parser.add_option(
        '-k','--key',
        dest='key',
        help='''Python expression used to compute the key for each
            line, "lambda line:" is prepended.\n
            Example : -k "line[5:10]". By default, the whole line is the key.'''
    )
    parser.add_option(
        '-t','--tempdir',
        dest='tempdirs',
        action='append',
        default=[],
        help='''Temporary directory to use. You might get performance
            improvements if the temporary directory is not on the same physical
            disk than the input and output directories. You can even try
            providing multiples directories on differents physical disks.
            Use multiple -t options to do that.'''
    )
    parser.add_option(
        '-p','--psyco',
        dest='psyco',
        action='store_true',
        default=False,
        help='''Use Psyco.'''
    )
    options,args = parser.parse_args()

    if options.key:
        # MK_NOTE: Note use of eval() to convert string to expression.
        # [GOOD|BAD] Idea?
        options.key = eval('lambda line : (%s)'%options.key)

    if options.psyco:
        import psyco
        psyco.full()

    batch_sort(args[0],args[1],options.key,options.buffer_size,options.tempdirs)
