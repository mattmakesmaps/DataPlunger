__author__ = 'matt'
__date__ = '4/4/14'
from dataplunger import readers

if __name__ == '__main__':
    delimiter = ','
    encoding = 'UTF-8'
    kwargs = {'type': 'ReaderCSV', 'delimeter': ','}
    path = '/Users/matt/Projects/dataplunger/sample_data/grades.csv'

    csvReader = readers.ReaderCSV(path,encoding,delimiter,**kwargs)
    # with readers.ReaderCSV(path, encoding, delimiter, **kwargs) as csvReader:
    #     for row in csvReader:
    #         pass
