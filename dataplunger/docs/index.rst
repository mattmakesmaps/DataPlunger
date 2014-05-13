.. . documentation master file, created by
   sphinx-quickstart on Thu May  1 19:46:53 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

DataPlunger
===========

DataPlunger is a prototype ETL processing toolchain.

The goal is to create a modular package for the purpose of extracting data from multiple backing stores,
performing n-number of transformational processing steps on those records, with the final output being loaded into
a new format.

A workflow, or processing pipeline, is defined via a JSON configuration file containing the following information:

* Connection information to source data for processing.
* Processing steps to be applied to individual records extracted from source.

Source code for this project can be found at: `<https://github.com/mattmakesmaps/DataPlunger>`_

**Install Instructions**::

    # Create virtualenv
    $ mkvirtualenv dp_dev_test
    (dp_dev_test)$ cd /path/to/DataPlunger
    # Install in development mode (sym-link to site-packages)
    (dp_dev_test)$ python setup.py develop


Configuration
-------------

Processing pipelines are described using a JSON configuration file.

.. toctree::
   :maxdepth: 2

   conf_file

Main Modules
------------

The DataPlunger package is broken down into three main modules, :doc:`dataplunger.core`, :doc:`dataplunger.processors`,
and :doc:`dataplunger.readers`.

**Core** contains configuration and control code.

.. toctree::

   dataplunger.core

**Processors** perform actions on a collection records.

.. toctree::

   dataplunger.processors

**Readers** are responsible for creating a connection to a backing datasource,
and returning an iterable that yields a single record of data from that datasource.

.. toctree::

   dataplunger.readers

Test Coverage
-------------

Unit tests currently cover the ``processors`` and ``readers`` modules.

.. toctree::

   dataplunger.tests.rst


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

