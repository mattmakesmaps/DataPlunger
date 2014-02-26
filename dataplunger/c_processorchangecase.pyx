def process_changecase(dict inLine, bytes case):
    # NOTE: Need to check for None type first.
    # Testing speed-up by declaring variable in local scope.
    # inLine = {key: self.changeCase(value, case) for key, value in inLine.iteritems()}
    cdef list fields
    cdef int recordslen=len(fields)
    cdef bytes key
    cdef int i
    fields = inLine.items()
    for i in range(recordslen):
        key = fields[i][0]
        if isinstance(fields[i][1], str):
            if case == 'lower':
                inLine[key] = fields[i][1].lower()
            else:
                inLine[key] = fields[i][1].upper()
    # for key, value in inLine.viewitems():
    #     if isinstance(value, str):
    #         if case == 'lower':
    #             inLine[key] = value.lower()
    #         else:
    #             inLine[key] = value.upper()
    return inLine
