__author__ = 'mkenny'

class ProcessorScreenWriter(object):
    """
    A Processor class that simply prints contents of a line.
    """
    def __init__(self, processor, **kwargs):
        self.processor = processor

    def process(self, inLine):
        print inLine
        self.processor.process(inLine)


class ProcessorChangeCase(object):
    """
    A Processor class which implements a public interface, the process() method.
    Responsible for changing case of values.
    """
    # NOTE: Passing a default value for self.case allows us
    # To not require it as an attribute for every layer.
    def __init__(self, processor, case=None, **kwargs):
        self.processor = processor
        self.case = case

    def process(self, inLine):
        # NOTE: Need to check for None type first.
        if self.case is None:
            self.processor.process(inLine)
        elif self.case.lower() == 'upper':
            inLine = {key: value.upper() for key, value in inLine.iteritems() if isinstance(value, str)}
            self.processor.process(inLine)
        elif self.case.lower() == 'lower':
            inLine = {key: value.lower() for key, value in inLine.iteritems() if isinstance(value, str)}
            self.processor.process(inLine)
        else:
            raise ValueError("Case Not Supported")

class ProcessorTruncateFields(object):
    """
    A decorator class which implements a Processor class' public
    interface, the process() method.
    """
    def __init__(self, processor, fields, **kwargs):
        self.processor = processor
        self.out_fields = fields

    def process(self, inLine):
        """
        Perform dict comprehension to create a dictionary subset to out_fields only.
        """
        truncated_line = {key: value for key, value in inLine.iteritems() if key in self.out_fields}
        self.processor.process(truncated_line)
