__author__ = 'matt'
__date__ = '1/31/14'
"""
A Reader class implements a context manager and __iter__ method. It can be thought of as an
example data connection.

Processing classes are examples of class-based decorator patterns. These perform work on
records extracted during a single iteration of a Reader class instance.

A Controller class is composed of a Reader class and any number of Processing class references.
"""

class ListReader:
    """
    An implementation of a file-like object that contains context
    manager methods __enter__ and __exit__, and an __iter__ method
    implemented as a generator.
    """
    def __init__(self, data):
        # NOTE: self.data would realistically be populated dynamically
        # after instanciation of the class. The __init__ method would
        # typically contain things like a db connection string.
        self.data = data

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # http://www.itmaybeahack.com/book/python-2.6/html/p03/p03c07_contexts.html
        if exc_type is not None:
            # Exception occurred
            return False # Will raise the exception
        return True # Everything's okay

    def __iter__(self):
        for datum in self.data:
            yield datum


class ProcessorScreenWriter:
    """
    A class which implements a file-like object's write() method.
    Simply prints contents of a line.
    """
    def __init__(self, writer):
        self.writer = writer

    def write(self, inLine):
        print inLine
        self.writer.write(inLine)


class ProcessorChangeCase:
    """
    A decorator class which implements a writer class' public
    interface, the write() method.
    """
    def __init__(self, writer, case):
        self.writer = writer
        self.case = case

    def write(self, inLine):
        if self.case.lower() == 'upper':
            self.writer.write(inLine.upper())
        elif self.case.lower() == 'lower':
            self.writer.write(inLine.lower())
        else:
            raise ValueError("Case Not Supported")


class ProcessorTruncateToFirstChar:
    """
    A decorator class which implements a writer class' public
    interface, the write() method.
    """
    def __init__(self, writer):
        self.writer = writer

    def write(self, inLine):
        firstElem = inLine[0]
        self.writer.write(firstElem)


class ProcessorAddFoo:
    """
    A decorator class which implements a writer class' public
    interface, the write() method.
    """
    def __init__(self, writer):
        self.writer = writer

    def write(self, inLine):
        fooLine = inLine + "fooooo"
        self.writer.write(fooLine)


class ProcessorExplodeFoo:
    """
    A decorator class which implements a writer class' public
    interface, the write() method.
    """
    def __init__(self, writer):
        self.writer = writer

    def _explode(self, feature):
        index = 0
        explodedFeatures = []
        for element in feature:
            index += 1
            element = str(element) + " Feature: " + str(index)
            explodedFeatures.append(element)
        return explodedFeatures

    def write(self, inLine):
        explodedFeatures = self._explode(inLine)
        for feature in explodedFeatures:
            self.writer.write(feature)


class DevNull:
    """
    A decorator class intended to be implemented at the last step
    of the processing chain. simply ends the workflow.
    """
    def write(self, inLine):
        pass


class NoProcessorException(Exception):
    """
    An exception raised when no processors are passed to
    the controller.
    """
    pass


class MainController:
    """
    A control class which is composed of a reader
    and n-number of processors.
    """
    def __init__(self, reader, processors=None):
        self.reader = reader
        if processors is None:
            raise NoProcessorException("ERROR: No Processors Found.")
        else:
            # Add an instance of ProcessorDevNull, ensuring process ends.
            self.processors = processors
            self.processors.reverse()

    def serialize(self):
        """
        Using the reader classes' context manager, loop through
        a reader's contents using the reader's __iter__() method.
        Output is written (serialized) using the writer class's
        write() method.
        """
        with self.reader as local_reader:
            for record in local_reader:
                decorated_processor = DevNull()
                for processor, args in self.processors:
                    if args:
                        decorated_processor = processor(decorated_processor, *args)
                    else:
                        decorated_processor = processor(decorated_processor)
                decorated_processor.write(record)


if __name__ == '__main__':
    myDataList = ['matt', 'roger', 'greg']
    myDataString = 'matt roger greg'

    # For each record: Print it, Explode It, Convert That to Upper Case, Print That
    # NOTE: each element of the processingSteps list is a two-element tuple containing
    # The processor class to be executed and either a list of positional arguments OR
    # None. This is currently required due to tuple unpacking inside the contoller class'
    # serialize method.
    processingSteps = [(ProcessorScreenWriter, None),
                       (ProcessorExplodeFoo, None),
                       (ProcessorChangeCase,['upper']),
                       (ProcessorScreenWriter, None)]

    myControllerWithProcessor = MainController(ListReader(myDataList), processingSteps)
    myControllerWithProcessor.serialize()

