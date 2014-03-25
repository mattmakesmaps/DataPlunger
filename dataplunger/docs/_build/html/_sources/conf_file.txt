Configuration File
==================

A processing workflow is outlined using a configuration file. This JSON-encoded file contains the following elements:

Example Configuration File
--------------------------

An example code block is as follows::

    {
        "type": "ConfigCollection",
        "configs": [
            {
                "name": "Sex By Age",
                "conn_info": {
                    "type": "ReaderCensus",
                    "path": "/Users/matt/Projects/dataplunger/sample_data/Washington_All_Geographies_Tracts_Block_Groups_Only",
                    "delimiter": ",",
                    "encoding": "UTF-8",
                    "fields": {
                        "Total": 1,
                        "Male": 2,
                        "Female": 17
                    },
                    "sequence": 2,
                    "starting_position": 87
                },
                "layers": {
                    "Sex_By_Age": {
                        "record_processing_steps": [
                            {"ProcessorMatchValue": {
                                "matches":{"SUMLEVEL":140},
                                "action":"Keep"
                            }},
                            {"ProcessorTruncateFields": {"fields": ["Total", "Male", "Female", "SUMLEVEL", "LOGRECNO"]}},
                            {"ProcessorScreenWriter": null}
                        ],
                        "aggregate_processing_steps": [
                            {"AggregateProcessorCSVWriter": {"path":"/Users/matt/Projects/dataplunger/sample_output/age_by_sex.csv",
                                "fields": ["Total", "Male", "Female", "SUMLEVEL", "LOGRECNO"]}}
                        ]
                    }
                }
            }
        ]
    }

Parameters
----------

Config Collection
+++++++++++++++++

``type`` - Defaults to ``ConfigCollection``. *In the future this may be expanded to also include a value of* ``Config``.

``configs`` - An array of individual config objects. *In the future, if* ``type`` *param has a value of* ``Config`` *,
this parameter would not be necessary.*

Config Object
+++++++++++++

``name`` - String. The name of the configuration.

``conn_info`` - Object. Contains user-provided parameters for the specific Reader instance.
At minimum, a value of ``type`` is required to be populated with the name of the desired reader class.
See Reader class specific documentation for a list of available Reader implementations and their required parameters.

``layers`` - Object. Attribute names represent names of individual output layers to be produced from the given reader. 

Layers
++++++

``layers`` are the end result of a series of user-defined processing steps performed against records output by
an instance of a Reader class. ``layers`` objects are comprised of two attributes: ``record_processing_steps``
and ``aggregate processing_steps``.

``record_processing_steps`` - An array containing references to objects that represent instances subclassed from ProcessorBaseClass.
These processing steps are implemented on a per-record level. Each record output from a given Reader object is run through each
Processor in the array, in the order defined by the array.

``aggregate_processing_steps`` - An array containing references to objects that represent instances subclassed
from AggregateProcessorBaseClass. After all records output from a Reader object have been processed on an individual,
per-record level, the Aggregate Processors listed in this array will be executed against the entire set of records.
