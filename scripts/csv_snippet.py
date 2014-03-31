__author__ = 'matt'
__date__ = '3/30/14'
"""
Test direct connection of reader and CSV writer.
"""
from dataplunger import readers, processors
import os

if __name__ == '__main__':
    conn_info = {
            "type": "ReaderCSV",
            "path": "/Users/matt/Projects/dataplunger/sample_data/sample_data.csv",
            "delimeter": ",",
            "encoding": "UTF-8"
        }

    data = [
        {'id':0,'name':'matt','gender':'male','city':'Seattle','nuller':'','nuller2':''},
        {'id':1,'name':'riley','gender':'female','city':'Seattle','nuller':'a','nuller2':''},
        {'id':2,'name':'Scott','gender':'male','city':'Bellingham','nuller':'a','nuller2':''}
    ]

    devNull = processors.ProcessorDevNull()
    out_path = os.path.join(os.path.dirname(__file__), "csv_snippet_out.csv")
    c_writer = processors.ProcessorCSVWriter(devNull, out_path, ['id','name','gender','city','nuller','nuller2'])
    c_writer.process(data)

    # with readers.ReaderCSV(conn_info) as t_reader:
    #     devNull = processors.ProcessorDevNull()
    #     out_path = os.path.join(os.path.dirname(__file__), "csv_snippet_out.csv")
    #     c_writer = processors.ProcessorCSVWriter(devNull, out_path, ['id','name','gender','city','nuller','nuller2'])
    #     c_writer.process(t_reader)
