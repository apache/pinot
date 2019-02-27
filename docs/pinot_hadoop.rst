..
.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.
..

Creating Pinot segments
=======================

Pinot segments can be created offline on Hadoop, or via command line from data files. Controller REST endpoint
can then be used to add the segment to the table to which the segment belongs.

Creating segments using hadoop
------------------------------

.. figure:: img/Pinot-Offline-only-flow.png

  Offline Pinot workflow

To create Pinot segments on Hadoop, a workflow can be created to complete the following steps:

#. Pre-aggregate, clean up and prepare the data, writing it as Avro format files in a single HDFS directory
#. Create segments
#. Upload segments to the Pinot cluster

Step one can be done using your favorite tool (such as Pig, Hive or Spark), Pinot provides two MapReduce jobs to do step two and three.

Configuring the job
^^^^^^^^^^^^^^^^^^^

Create a job properties configuration file, such as one below:

.. code-block:: none

  # === Index segment creation job config ===

  # path.to.input: Input directory containing Avro files
  path.to.input=/user/pinot/input/data

  # path.to.output: Output directory containing Pinot segments
  path.to.output=/user/pinot/output

  # path.to.schema: Schema file for the table, stored locally
  path.to.schema=flights-schema.json

  # segment.table.name: Name of the table for which to generate segments
  segment.table.name=flights

  # === Segment tar push job config ===

  # push.to.hosts: Comma separated list of controllers host names to which to push
  push.to.hosts=controller_host_0,controller_host_1

  # push.to.port: The port on which the controller runs
  push.to.port=8888


Executing the job
^^^^^^^^^^^^^^^^^

The Pinot Hadoop module contains a job that you can incorporate into your
workflow to generate Pinot segments.

.. code-block:: none

  mvn clean install -DskipTests -Pbuild-shaded-jar
  hadoop jar pinot-hadoop-0.016-shaded.jar SegmentCreation job.properties

You can then use the SegmentTarPush job to push segments via the controller REST API.

.. code-block:: none

  hadoop jar pinot-hadoop-0.016-shaded.jar SegmentTarPush job.properties


Creating Pinot segments outside of Hadoop
-----------------------------------------

Here is how you can create Pinot segments from standard formats like CSV/JSON.

#. Follow the steps described in the section on :ref:`compiling-code-section` to build pinot. Locate ``pinot-admin.sh`` in ``pinot-tools/target/pinot-tools=pkg/bin/pinot-admin.sh``.
#.  Create a top level directory containing all the CSV/JSON files that need to be converted into segments.
#.  The file name extensions are expected to be the same as the format name (*i.e* ``.csv``, or ``.json``), and are case insensitive.
    Note that the converter expects the ``.csv`` extension even if the data is delimited using tabs or spaces instead.
#.  Prepare a schema file describing the schema of the input data. The schema needs to be in JSON format. See example later in this section.
#.  Specifically for CSV format, an optional csv config file can be provided (also in JSON format). This is used to configure parameters like the delimiter/header for the CSV file etc.
        A detailed description of this follows below.

Run the pinot-admin command to generate the segments. The command can be invoked as follows. Options within "[ ]" are optional. For -format, the default value is AVRO.

.. code-block:: none

    bin/pinot-admin.sh CreateSegment -dataDir <input_data_dir> [-format [CSV/JSON/AVRO]] [-readerConfigFile <csv_config_file>] [-generatorConfigFile <generator_config_file>] -segmentName <segment_name> -schemaFile <input_schema_file> -tableName <table_name> -outDir <output_data_dir> [-overwrite]


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

.. code-block:: none

  {
    "fileFormat" : "EXCEL",
    "header" : "col1,col2,col3,col4",
    "delimiter" : "\t",
    "dateFormat" : "mm-dd-yy"
    "dateColumns" : ["col1", "col2"]
  }

Sample Schema:

.. code-block:: none

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

Pushing segments to Pinot
^^^^^^^^^^^^^^^^^^^^^^^^^

You can use curl to push a segment to pinot:

.. code-block:: none

   curl -X POST -F segment=@<segment-tar-file-path> http://controllerHost:controllerPort/segments


Alternatively you can use the pinot-admin.sh utility to upload one or more segments:

.. code-block:: none

  pinot-tools/target/pinot-tools-pkg/bin//pinot-admin.sh UploadSegment -controllerHost <hostname> -controllerPort <port> -segmentDir <segmentDirectoryPath>

The command uploads all the segments found in ``segmentDirectoryPath``.
The segments could be either tar-compressed (in which case it is a file under ``segmentDirectoryPath``)
or uncompressed (in which case it is a directory under ``segmentDirectoryPath``).
