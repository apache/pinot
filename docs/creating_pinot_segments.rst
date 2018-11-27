Creating Pinot segments outside of Hadoop
=========================================

This document describes steps required for creating Pinot2_0 segments from standard formats like CSV/JSON.

Compiling the code
------------------
Follow the steps described in the section on :doc: `Demonstration <trying_pinot>` to build pinot. Locate ``pinot-admin.sh`` in ``pinot-tools/trget/pinot-tools=pkg/bin/pinot-admin.sh``.


Data Preparation
----------------

#.  Create a top level directory containing all the CSV/JSON files that need to be converted.
#.  The file name extensions are expected to be the same as the format name (_i.e_ ``.csv``, or ``.json``), and are case insensitive.
    Note that the converter expects the .csv extension even if the data is delimited using tabs or spaces instead.
#.  Prepare a schema file describing the schema of the input data. This file needs to be in JSON format. An example is provided at the end of the article.
#.  Specifically for CSV format, an optional csv config file can be provided (also in JSON format). This is used to configure parameters like the delimiter/header for the CSV file etc.  
        A detailed description of this follows below.  

Creating a segment
------------------
Run the pinot-admin command to generate the segments. The command can be invoked as follows. Options within "[ ]" are optional. For -format, the default value is AVRO.

::

    bin/pinot-admin.sh CreateSegment -dataDir <input_data_dir> [-format [CSV/JSON/AVRO]] [-readerConfigFile <csv_config_file>] [-generatorConfigFile <generator_config_file>] -segmentName <segment_name> -schemaFile <input_schema_file> -tableName <table_name> -outDir <output_data_dir> [-overwrite]

CSV Reader Config file
----------------------
To configure various parameters for CSV a config file in JSON format can be provided. This file is optional, as are each of its parameters. When not provided, default values used for these parameters are described below:

#.  fileFormat: Specify one of the following. Default is EXCEL.  

 ##.  EXCEL
 ##.  MYSQL
 ##.  RFC4180
 ##.  TDF

#.  header: If the input CSV file does not contain a header, it can be specified using this field. Note, if this is specified, then the input file is expected to not contain the header row, or else it will result in parse error. The columns in the header must be delimited by the same delimiter character as the rest of the CSV file.
#.  delimiter: Use this to specify a delimiter character. The default value is ",".
#.  dateFormat: If there are columns that are in date format and need to be converted into Epoch (in milliseconds), use this to specify the format. Default is "mm-dd-yyyy".
#.  dateColumns: If there are multiple date columns, use this to list those columns.

Below is a sample config file.

::

  {
    "fileFormat" : "EXCEL",
    "header" : "col1,col2,col3,col4",
    "delimiter" : "\t",
    "dateFormat" : "mm-dd-yy"
    "dateColumns" : ["col1", "col2"]
  }

Sample JSON schema file:

::

  {
    "dimensionFieldSpecs" : [
      {			   
        "dataType" : "STRING",
        "delimiter" : null,
        "singleValueField" : true,
        "name" : "name"
      },
      {
        "dataType" : "INT",
        "delimiter" : null,
        "singleValueField" : true,
        "name" : "age"
      }
    ],
    "timeFieldSpec" : {
      "incomingGranularitySpec" : {
        "timeType" : "DAYS",
        "dataType" : "LONG",
        "name" : "incomingName1"
      },
      "outgoingGranularitySpec" : {
        "timeType" : "DAYS",
        "dataType" : "LONG",
        "name" : "outgoingName1"
      }
    },
    "metricFieldSpecs" : [
      {
        "dataType" : "FLOAT",
        "delimiter" : null,
        "singleValueField" : true,
        "name" : "percent"
      }
     ]
    },
    "schemaName" : "mySchema",
  }
