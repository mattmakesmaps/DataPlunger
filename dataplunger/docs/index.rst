.. . documentation master file, created by
   sphinx-quickstart on Thu May  1 19:46:53 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

DataPlunger
===========

DataPlunger is a prototype ETL processing toolchain.

The goal is to create a modular code for the purpose of extracting data from multiple backing stores,
performing n-number of transformational processing steps on those records, with the final output being loaded into
a new format.

A workflow, or processing pipeline, is defined via a JSON configuration file containing the following information:

* Connection information to source data for processing.
* Processing steps to be applied to individual records extracted from source.

Source code for this project can be found at: `<https://github.com/mattmakesmaps/DataPlunger>`_

Configuration
-------------

Processing pipelines are described using a JSON configuration file.

.. toctree::
   :maxdepth: 2

   conf_file

Main Modules
------------

:doc:`dataplunger.core` - Code for parsing a JSON configuration file, building a processing pipeline,
and executing it.

:doc:`dataplunger.readers` - Connections to backing datastores (Postgres, CSV, SHP, etc).

:doc:`dataplunger.processors` - Tools designed to execute a on either a single record, or an aggregate of records.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

