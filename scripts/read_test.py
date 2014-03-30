__author__ = 'matt'
__date__ = '3/28/14'
import os
from dataplunger.readers import ReaderCSV
from datetime import datetime

if __name__ == '__main__':
    conn_info = {
        "type": "ReaderCSV",
        "path": os.path.join(os.path.dirname(os.path.dirname(__file__)), "sample_data/NATIONAL_SINGLE.CSV"),
        "delimeter": ",",
        "encoding": "UTF-8"
    }
    r_list = []

    start_time = datetime.now()
    with ReaderCSV(conn_info) as t_reader:
        i = 0
        for record in t_reader:
            r_list.append(record)
            i += 1
            if i % 100000 == 0:
                print "Records: %s Time %s" % (i, datetime.now()-start_time)
    total_time = datetime.now()-start_time
    # One Run Total Time: 0:00:44.713295
    print "Total Time: %s" % total_time
    # Total Time: 0:01:02.418729
    # Sort Time: 0:01:17.841382
    sorted_list = sorted(r_list, key=lambda record: record['PRIMARY_NAME'])
    sort_time = datetime.now()-start_time
    # One Run Total Time: 0:00:44.713295
    print "Sort Time: %s" % sort_time
