#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. module:: readers.py
   :platform: Unix
   :synopsis: Readers connect to data and return individual records.

.. moduleauthor:: Matt

Readers are responsible for creating a connection to a backing datasource,
and returning an iterable that yields a single record of data from that datasource.

Readers emit a single record via the __iter__() method. A single record is represented by
a dictionary. Each element's key name represents a field names, and each value represents
the value for that key.

For values, strings should be emitted as Unicode objects. Numerical values may be emitted
using an appropriate Python data type.

Currently, readers that support geometry emit geoms using the standard set by Fiona_.
Geometries are represented as lists of two-element tuples, representing a single coordinate pair.

.. _Fiona: http://toblerity.org/fiona/manual.html#record-geometry
"""
__author__ = 'mkenny'
import abc
import csv
import os
import fiona
import psycopg2
from psycopg2.extras import RealDictCursor


class ReaderBaseClass(object):
    """An abstract base class for a Reader interface."""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, **kwargs):
        """Assign parameters extracted from a configuration file
        to parameters on the instance."""
        pass

    @abc.abstractmethod
    def __del__(self, exc_type=None, exc_val=None, ext_tb=None):
        """Cleanup code performed when the instance is garbage collected.
        Note: will be called twice if a context manager is used."""
        pass

    def __enter__(self):
        """Setup code called when instantiated via context manager."""
        return self

    def __exit__(self, exc_type, exc_val, ext_tb):
        """Cleanup code called when instantiated via context manager."""
        return self.__del__(exc_type, exc_val, ext_tb)

    @abc.abstractmethod
    def __iter__(self):
        """Return a generator object yielding individual records."""
        pass


class ReaderSHP(ReaderBaseClass):
    """
    Reader class implementation for SHP files using Fiona.
    Returns a flattened Fiona record. See: http://toblerity.org/fiona/manual.html#records
    Fiona generated dictionary keys id and type are transformed into key names,
    fiona_id and fiona_type.

    Required Config Parameters:

    :param path: Attribute containing the actual file path.

    Example configuration file entry::

            "NaturalEarthLakes": {
                "type": "ReaderSHP",
                "path": "/Users/matt/Projects/dataplunger/tests/test_data/50m_lakes_utf8.shp"
            }
    """

    def __init__(self, path, **kwargs):
        """
        :param path: Attribute containing the actual file path.
        """
        self.path = path
        self._shp_reader = fiona.open(path, 'r')

    def __iter__(self):
        """
        Generator returning a dict of field name: field value pairs for each record.

        Attributes extracted from the shapefile are returned by Fiona as unicode objects by default.
        Fiona specific attributes, id and type, are manually converted to unicode.
        """
        for row in self._shp_reader:
            # Flatten the Fiona returned record.
            flat_dict = row['properties']
            flat_dict['geometry'] = row['geometry']
            flat_dict['fiona_id'] = unicode(row['id'])
            flat_dict['fiona_type'] = unicode(row['type'])
            yield flat_dict

    def __del__(self, exc_type=None, exc_val=None, exc_tb=None):
        """Close the file handler. Note: Will be Called Twice if a Context Manager is used."""
        if self._shp_reader:
            self._shp_reader.close()
        if exc_type is not None:
            # Exception occurred
            return False  # Will raise the exception
        return True  # Everything's okay


class ReaderCSV(ReaderBaseClass):
    """
    Reader class implementation for CSV files.
    
    Required Config Parameters:
    
    :param delimiter: Character representing delimiter. Defaults to ','
    :param path: Attribute containing the actual file path.

    Non-Required Config Parameters:

    :param field_types: a mapping of field names to output python data type.
        if not provided, defaults all output to strings.

    Example configuration file entry::

            "Grades": {
                "type": "ReaderCSV",
                "path": "/Users/matt/Projects/dataplunger/sample_data/people.csv",
                "delimiter": ",",
                "field_types": {'name':'string', 'age':'int', 'gender':'string'}
            },
    """
    def __init__(self, path, delimiter=',', field_types=None, **kwargs):
        """
        :param path: the pathway for a given file.
        :param delimiter:  defaults to ','
        :param field_types: a dict of field name, field type pairs.
        :param _file_handler:  set in __enter__(), a read only pointer to the CSV.
        :param _dict_reader:  an instance of csv.dict_reader()
        """
        # If no delimiter given in config, default to ','
        self.delimiter = delimiter
        self.path = path
        self.field_types = field_types
        self._file_handler = open(self.path, 'rt')
        self._dict_reader = csv.DictReader(self._file_handler, delimiter=self.delimiter)

    def _map_field_types(self, row):
        """
        Return a record updated to reflect data types listed in self.field_types.
        TODO: Add more types.
        """
        field_mapping = {
            'int': int,
            'integer': int,
            'str': unicode,
            'string': unicode,
            'unicode': unicode,
            'text': unicode,
            'float': float
        }
        for field_name, field_type in self.field_types.iteritems():
            row[field_name] = field_mapping[field_type](row[field_name])
        return row

    def __iter__(self):
        """
        Generator returning a dict of field name: field value pairs for each record.
        """
        for row in self._dict_reader:
            # Cast fields to proper type, if given.
            if self.field_types:
                row = self._map_field_types(row)
            yield row

    def __del__(self, exc_type=None, exc_val=None, exc_tb=None):
        """Close the file handler. Note: Will be Called Twice if a Context Manager is used."""
        if self._file_handler:
            self._file_handler.close()
        if exc_type is not None:
            # Exception occurred
            return False  # Will raise the exception
        return True  # Everything's okay

    def __enter__(self):
        """Return Self When Called As Context Manager"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Execute __del__() when called using a context manager."""
        return self.__del__(exc_type, exc_val, exc_tb)


class ReaderCensus(ReaderBaseClass):
    """
    Reader class implementation for Census ACS Summary Files.

    Required Config Parameters:

    :param path: contains a pathway to an ACS formatted directory of estimate and geography tables.
    :param fields: an object containing two-element object pairs for "line_number" and "type".
        "line_number" is extracted from, 'Sequence_Number_and_Table_Number_Lookup.xls'.
        "type" refers to a field type, e.g. int, string, float.
    :param sequence: Sequence No. From, 'Sequence_Number_and_Table_Number_Lookup.xls'
    :param starting_position: Starting Position for table of int.
        From, 'Sequence_Number_and_Table_Number_Lookup.xls'

    Non-Required Config Parameters:

    :param delimiter: delimiter for CSV file, defaults to comma.

    Example configuration file entry::

            "WA_All": {
                "type": "ReaderCensus",
                "path": "path/to/Washington_All_Geographies_Tracts_Block_Groups_Only",
                "delimiter": ",",
                "fields": {
                    'Total': {"line_number": 1, "type": "int"},
                    'Female': {"line_number": 17, "type": "int"},
                    'Male': {"line_number": 2, "type": "int"}
                },
                "sequence": 2,
                "starting_position": 87
            }
    """

    def __init__(self, fields, path, sequence, starting_position, delimiter=",", **kwargs):
        """
        :param delimiter: extracted from conn_info, defaults to ','.
        :param fields: dict of field names and line number indexes.
        :param path: location of directory of census data.
        :param sequence: sequence number for table of interest.
        :param starting_position: starting position for table of interest.
        :param _estimate_reader: a list of csv.reader instances parsing estimate tables.
        :param _estimate_path: a list of paths to estimate files, generated based on
            user provided path and sequence value.
        :param _geography_path: a list of paths to geography file, generated based on user provided path.
        :param _geography_records: populated by parsing a geography file.
            a dictionary populated with keys representing LOGRECNO values
            for a given row, and values representing the first six fields
            of that row.
        """
        self.delimiter = delimiter
        self.fields = fields
        self.path = path
        self.sequence = sequence
        self.starting_position = starting_position
        self._estimate_handlers = []
        self._estimate_reader = []
        self._estimate_path = []
        self._geography_path = []
        self._geography_records = {}

        # Call internal setup function.
        self._setup()

    def _setup(self):
        self._get_paths()
        self._build_logrecno_dict()
        self._build_estimate_reader()
        return True

    def _get_paths(self):
        """
        Build paths for the geography and estimate files.
        Geography files begin a 'g' and have a CSV extension.
        Estimate files are based on the sequence number.
        """
        dir_contents = os.listdir(self.path)
        sequence_num = int(self.sequence)
        for f in dir_contents:
            # Get geography path. Slice doesn't need to be trapped for index error due to string < 3 char.
            if f[0] == 'g' and f[len(f)-3:] == 'csv':
                self._geography_path.append(os.path.join(self.path, f))
            # Set estimate path. Pass over when string is too short (index error) or
            # when characters f[8:12] are not coercible to integers (Value Error).
            if f[0] == 'e':
                try:
                    if int(f[8:12]) == sequence_num:
                        self._estimate_path.append(os.path.join(self.path, f))
                except (ValueError, IndexError):
                    pass

        # Raise Errors if not populated.
        if not self._geography_path:
            raise IOError("Expected geography file not found. Starts with 'g' and csv extent")
        if not self._estimate_path:
            raise IOError("Expected estimate file not found. Sequence given: %s." % self.sequence)

    def _build_logrecno_dict(self):
        """
        Create a dictionary of LOGRECNO values with associated attributes.
        Will be used as a lookup during iteration of estimate table.

        Note that census data are expected to be encoded in Latin-1.
        """
        for f in self._geography_path:
            with open(f, 'rt') as geography_file_handle:
                geography_field_names = ['FILEID', 'STUSAB', 'SUMLEVEL', 'COMPONENT', 'LOGRECNO']
                geography_reader = csv.DictReader(geography_file_handle, geography_field_names, delimiter=self.delimiter)

                for record in geography_reader:
                    """
                    Just using index for GEOID and NAME column.
                    Alternative would have to be to pass a 50+ length list
                    of all field names to geography_reader constructor.
                    FIPS is a concatenation of multiple geographic IDs.
                    """
                    record_key = record['STUSAB'] + record['LOGRECNO']
                    self._geography_records[record_key] = {
                        'COMPONENT': unicode(record['COMPONENT'], 'latin-1'),
                        'FILEID': unicode(record['FILEID'], 'latin-1'),
                        'LOGRECNO': unicode(record['LOGRECNO'], 'latin-1'),
                        'STUSAB': unicode(record['STUSAB'], 'latin-1'),
                        'SUMLEVEL': unicode(record['SUMLEVEL'], 'latin-1'),
                        'FIPS': unicode(''.join([record[None][4],record[None][5],record[None][8],
                                                 record[None][9]]), 'latin-1'),
                        'GEOID': unicode(record[None][43], 'latin-1'),
                        'NAME': unicode(record[None][44], 'latin-1')
                    }

    def _build_estimate_reader(self):
        """
        Create a CSV reader using the estimate file.
        Reformat starting_position and individual field indexes
        for use in list lookup.
        """
        # Reformat the user-provided field indexes with values reflecting starting_position
        # Decrement by two to account for both the user-provided starting position and the field values
        start_index = int(self.starting_position) - 2
        for k, v in self.fields.iteritems():
            self.fields[k]['line_number'] = start_index + int(v['line_number'])

        for f in self._estimate_path:
            _estimate_handler = open(f, 'rt')
            self._estimate_handlers.append(_estimate_handler)
            self._estimate_reader.append(csv.reader(_estimate_handler, delimiter=self.delimiter))

    def _field_mapper(self, value, field_type):
        """
        Return an input string coerced to a user-provided field type,
        as seen in the ReaderCensus.fields attribute of this class.
        """
        field_mapping = {
            'int': int,
            'integer': int,
            'str': unicode,
            'string': unicode,
            'unicode': unicode,
            'text': unicode,
            'float': float
        }
        if field_mapping[field_type] == unicode:
            mod_value = unicode(value, 'latin-1')
        else:
            mod_value = field_mapping[field_type](value)
        return mod_value

    def _build_estimate_vals(self, row, fields):
        """
        Build estimate values.

        ValueError exceptions (e.g. Jam Values) of ',' and empty string are converted to 0.
        Other ValueError exceptions are logged and converted to strings.

        This function replaces the following one-liner, adding an exception handler::

           {k: self._field_mapper(value=row[v['line_number']], field_type=v['type']) for k, v in fields.items()}
        """
        estimate_vals = {}
        for k, v in fields.iteritems():
            index = v['line_number']
            type = v['type']
            try:
                estimate_vals[k] = self._field_mapper(value=row[index], field_type=type)
            except ValueError as e:
                # Jam Values (Expected)
                if row[index] in ['', '.']:
                    #print('INFO: Jam Value Encountered in Field {0}: {1} Replacing with 0'.format(k, e))
                    estimate_vals[k] = 0
                # Non-Jam Values (Un-Expected)
                else:
                    print('Error: Unexpected Value in Estimate Field {0}: {1} Coercing to string'.format(k, e))
                    estimate_vals[k] = str(row[index])
        return estimate_vals

    def __del__(self, exc_type=None, exc_val=None, exc_tb=None):
        """
        Close estimate file handle.
        """
        # if self._estimate_handler:
        #     self._estimate_handler.close()
        if self._estimate_handlers:
            for f in self._estimate_handlers:
                f.close()
        if exc_type is not None:
            # Exception occurred
            return False  # Will raise the exception
        return True  # Everything's okay

    def __iter__(self):
        """
        Generator returning a dict of field name: field value pairs for each record.
        Combines estimate row with corresponding geography row based on common LOGRECNO value.
        NOTE: We assume all estimate values to return INTs.
        """
        for reader in self._estimate_reader:
            for row in reader:
                # concat state abbreviation and logrecno to get unique key
                stusab = row[2]
                logrecno = row[5]
                record_key = stusab.upper() + logrecno
                estimate_vals = self._build_estimate_vals(row, self.fields)
                # get the corresponding geographic record
                if record_key in self._geography_records:
                    # yield a concatenated estimate and geography dictionary
                    geography_vals = self._geography_records[record_key]
                    yield dict(estimate_vals.items() + geography_vals.items())
                else:
                    raise KeyError("LOGRECNO: %s not found in geography table." % str(record_key))


class ReaderPostgres(ReaderBaseClass):
    """
    Reader class implementation executing a single query via psycopg2.
    Specifically, the __iter__() method will yield a single dictionary
    output from an instance of the psycopg2.extras.RealDictCursor class.

    Required Config Parameters:

    :param query: Attribute containing either a file path to a sql statement,
        ending in '.sql'; or a sql statement as a string.
    :param database: name of the database to connect to.

    Non-Required Config Parameters:

    :param user: db user to connect as.
    :param password: password for db user.
    :param host: host name, defaults to localhost.
    :param port: port number, defaults to 5432.

    Example configuration file entry::

            "GeocodingCities": {
                "type": "ReaderPostgres",
                "query": "/Users/matt/Projects/dataplunger/sample_data/sample_query.sql,
                "database": "dbname",
                "user": "postgres",
                "password": "postgres",
                "host": "localhost",
                "port": 5432
            },
    """

    def __init__(self, query, database, user=None, password=None, host='localhost', port=5432, **kwargs):
        self.conn_params = {
            'database': database,
            'host': host,
            'port': port}
        # Append user and password if provided
        # Else i think this uses your system user. TODO: Check on that.
        if user:
            self.conn_params['user'] = user
        if password:
            self.conn_params['password'] = password
        self.query = query
        self._conn_handler = self._open_connection(self.conn_params)
        self._dict_cursor = self._execute_query(self._conn_handler, self.query)

    def _open_connection(self, conn_params):
        """Return an open psycopg2 connection"""
        conn = psycopg2.connect(cursor_factory=RealDictCursor, **conn_params)
        return conn

    def _validate_query(self, query):
        """Return validated query.
        Currently only tests if self.query param is a file or
        defaults to believing it has a well-formed query inline.
        TODO: Update to try and sanitize/validate the query somehow."""
        try:
            # Test if we have file.
            if os.path.isfile(query):
                with open(query, 'r') as query_file_handle:
                    query_text = query_file_handle.read()
            else:
                query_text = query
            return query_text
        except Exception:
            raise Exception

    def _execute_query(self, db_conn, query):
        """Return a cursor with result set from self.query"""
        # Test if self.query is a file for inline query
        validated_query = self._validate_query(query)
        # Create cursor with Unicode support and Execute Query
        cur = db_conn.cursor()
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
        cur.execute(validated_query)
        return cur

    def __del__(self, exc_type=None, exc_val=None, exc_tb=None):
        """Close the db connection. Note: Will be Called Twice if a Context Manager is used."""
        if self._conn_handler:
            self._conn_handler.close()
        if exc_type is not None:
            # Exception occurred
            return False  # Will raise the exception
        return True  # Everything's okay

    def __iter__(self):
        """Yield a single record back to the caller."""
        for row in self._dict_cursor:
            yield row
