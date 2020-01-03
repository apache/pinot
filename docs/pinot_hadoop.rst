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

.. _creating-segments:

Creating Pinot segments
=======================

Pinot segments can be created offline on Hadoop, or via command line from data files. Controller REST endpoint
can then be used to add the segment to the table to which the segment belongs. Pinot segments can also be created by
ingesting data from realtime resources (such as Kafka).

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

.. code-block:: bash

   mvn clean install -DskipTests -Pbuild-shaded-jar
   hadoop jar pinot-hadoop-<version>-SNAPSHOT-shaded.jar SegmentCreation job.properties

You can then use the SegmentTarPush job to push segments via the controller REST API.

.. code-block:: bash

   hadoop jar pinot-hadoop-<version>-SNAPSHOT-shaded.jar SegmentTarPush job.properties


Creating Pinot segments using Spark
-----------------------------------------

Similar to Pinot hadoop, you can create Pinot segments in Spark.

Configuring the job
^^^^^^^^^^^^^^^^^^^

Pinot Spark keeps same format of job configuration file, such as one below:

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

   # use.relative.path: Match output segments hierarchy along with input file hierarchy.
   # E.g. data files layout is:
   #    /user/pinot/input/data/2019/10/24/part-0.avro
   #    /user/pinot/input/data/2019/10/24/part-1.avro
   #    /user/pinot/input/data/2019/10/25/part-0.avro
   #    /user/pinot/input/data/2019/10/25/part-1.avro
   # Then output directory layout would be:
   #    /user/pinot/output/2019/10/24/flights_2019-10-24_2019-10-24_0.tar.gz
   #    /user/pinot/output/2019/10/24/flights_2019-10-24_2019-10-24_1.tar.gz
   #    /user/pinot/output/2019/10/25/flights_2019-10-25_2019-10-25_2.tar.gz
   #    /user/pinot/output/2019/10/25/flights_2019-10-25_2019-10-25_3.tar.gz
   #
   # use.relative.path=true

   # look.back.period.in.days: only process files within recent days.
   # For segment creation job, it creates segments for data files been modified within recent configured days.
   # For segment push job, it pushes segments created/updated within recent configured days.
   #
   # look.back.period.in.days=2

   # local.directory.sequence.id: when enabled, segment sequence id is assigned based on local directory,
   # not globally.
   # E.g. data files layout is:
   #    /user/pinot/input/data/2019/10/24/part-0.avro
   #    /user/pinot/input/data/2019/10/24/part-1.avro
   #    /user/pinot/input/data/2019/10/25/part-0.avro
   #    /user/pinot/input/data/2019/10/25/part-1.avro
   # Then sequence ids for
   #    `/user/pinot/input/data/2019/10/24/part-0.avro` is 0,
   #    `/user/pinot/input/data/2019/10/24/part-1.avro` is 1,
   #    `/user/pinot/input/data/2019/10/25/part-0.avro` is 0,
   #    `/user/pinot/input/data/2019/10/25/part-1.avro` is 1.
   # This is result segment name to be
   #    /user/pinot/output/2019/10/24/flights_2019-10-24_2019-10-24_0.tar.gz
   #    /user/pinot/output/2019/10/24/flights_2019-10-24_2019-10-24_1.tar.gz
   #    /user/pinot/output/2019/10/25/flights_2019-10-25_2019-10-25_0.tar.gz
   #    /user/pinot/output/2019/10/25/flights_2019-10-25_2019-10-25_1.tar.gz
   #
   # local.directory.sequence.id=true

   # === Segment tar push job config ===

   # push.to.hosts: Comma separated list of controllers host names to which to push
   push.to.hosts=controller_host_0,controller_host_1

   # push.to.port: The port on which the controller runs
   push.to.port=8888

   # enable.parallel.push: Push Segments in parallel
   enable.parallel.push=true

   # push.job.parallelism: Push job parallelism, works when `enable.parallel.push=true`
   push.job.parallelism=4

   # push.job.retry: How many retries for segment push failure before throw exceptions
   push.job.retry=3

Executing the job
^^^^^^^^^^^^^^^^^

The Pinot Spark module contains a job that you can incorporate into your
workflow to generate Pinot segments.

.. code-block:: bash

   mvn clean install -DskipTests -Pbuild-shaded-jar
   spark-submit --class org.apache.pinot.spark.PinotSparkJobLauncher \
   pinot-spark-<version>-SNAPSHOT-shaded.jar SegmentCreation job.properties

You can then use the SegmentTarPush job to push segments via the controller REST API.

.. code-block:: bash

   spark-submit --class org.apache.pinot.spark.PinotSparkJobLauncher \
   pinot-spark-<version>-SNAPSHOT-shaded.jar SegmentTarPush job.properties


Creating Pinot segments outside of Hadoop
-----------------------------------------

Here is how you can create Pinot segments from standard formats like CSV/JSON/AVRO.

#. Follow the steps described in the section on :ref:`compiling-code-section` to build pinot. Locate ``pinot-admin.sh`` in ``pinot-tools/target/pinot-tools=pkg/bin/pinot-admin.sh``.
#. Create a top level directory containing all the CSV/JSON/AVRO files that need to be converted into segments.
#. The file name extensions are expected to be the same as the format name (*i.e* ``.csv``, ``.json`` or ``.avro``), and are case insensitive. Note that the converter expects the ``.csv`` extension even if the data is delimited using tabs or spaces instead.
#. Prepare a schema file describing the schema of the input data. The schema needs to be in JSON format. See example later in this section.
#. Specifically for CSV format, an optional csv config file can be provided (also in JSON format). This is used to configure parameters like the delimiter/header for the CSV file etc. A detailed description of this follows below.

Run the pinot-admin command to generate the segments. The command can be invoked as follows. Options within "[ ]" are optional. For -format, the default value is AVRO.

.. code-block:: bash

   bin/pinot-admin.sh CreateSegment -dataDir <input_data_dir> [-format [CSV/JSON/AVRO]] [-readerConfigFile <csv_config_file>] [-generatorConfigFile <generator_config_file>] -segmentName <segment_name> -schemaFile <input_schema_file> -tableName <table_name> -outDir <output_data_dir> [-overwrite]

To configure various parameters for CSV a config file in JSON format can be provided. This file is optional, as are each of its parameters. When not provided, default values used for these parameters are described below:

#. fileFormat: Specify one of the following. Default is EXCEL.

   #. EXCEL
   #. MYSQL
   #. RFC4180
   #. TDF

#. header: If the input CSV file does not contain a header, it can be specified using this field. Note, if this is specified, then the input file is expected to not contain the header row, or else it will result in parse error. The columns in the header must be delimited by the same delimiter character as the rest of the CSV file.
#. delimiter: Use this to specify a delimiter character. The default value is ",".
#. multiValueDelimiter: Use this to specify a delimiter character for each value in multi-valued columns. The default value is ";".

Below is a sample config file.

.. code-block:: json

   {
     "fileFormat": "EXCEL",
     "header": "col1,col2,col3,col4",
     "delimiter": "\t",
     "multiValueDelimiter": ","
   }

Sample Schema:

.. code-block:: json

   {
     "schemaName": "flights",
     "dimensionFieldSpecs": [
       {
         "name": "flightNumber",
         "dataType": "LONG"
       },
       {
         "name": "tags",
         "dataType": "STRING",
         "singleValueField": false
       }
     ],
     "metricFieldSpecs": [
       {
         "name": "price",
         "dataType": "DOUBLE"
       }
     ],
     "timeFieldSpec": {
       "incomingGranularitySpec": {
         "name": "daysSinceEpoch",
         "dataType": "INT",
         "timeType": "DAYS"
       }
     }
   }

Pushing offline segments to Pinot
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can use curl to push a segment to pinot:

.. code-block:: bash

   curl -X POST -F segment=@<segment-tar-file-path> http://controllerHost:controllerPort/segments


Alternatively you can use the pinot-admin.sh utility to upload one or more segments:

.. code-block:: bash

   pinot-tools/target/pinot-tools-pkg/bin//pinot-admin.sh UploadSegment -controllerHost <hostname> -controllerPort <port> -segmentDir <segmentDirectoryPath>

The command uploads all the segments found in ``segmentDirectoryPath``.
The segments could be either tar-compressed (in which case it is a file under ``segmentDirectoryPath``)
or uncompressed (in which case it is a directory under ``segmentDirectoryPath``).

Realtime segment generation
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To consume in realtime, we simply need to create a table with the same name as the schema and point to the Kafka topic
to consume from, using a table definition such as this one:

.. code-block:: json

   {
     "tableName": "flights",
     "tableType": "REALTIME",
     "segmentsConfig": {
       "retentionTimeUnit": "DAYS",
       "retentionTimeValue": "7",
       "segmentPushFrequency": "daily",
       "segmentPushType": "APPEND",
       "replication": "1",
       "timeColumnName": "daysSinceEpoch",
       "timeType": "DAYS",
       "segmentAssignmentStrategy": "BalanceNumSegmentAssignmentStrategy"
     },
     "tableIndexConfig": {
       "invertedIndexColumns": [
         "flightNumber",
         "tags",
         "daysSinceEpoch"
       ],
       "loadMode": "MMAP",
       "streamConfigs": {
         "streamType": "kafka",
         "stream.kafka.consumer.type": "highLevel",
         "stream.kafka.topic.name": "flights-realtime",
         "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder",
         "stream.kafka.zk.broker.url": "localhost:2181",
         "stream.kafka.hlc.zk.connect.string": "localhost:2181"
       }
     },
     "tenants": {
       "broker": "brokerTenant",
       "server": "serverTenant"
     },
     "metadata": {
     }
   }

First, we'll start a local instance of Kafka and start streaming data into it:

.. code-block:: bash

   bin/pinot-admin.sh StartKafka &
   bin/pinot-admin.sh StreamAvroIntoKafka -avroFile flights-2014.avro -kafkaTopic flights-realtime &

This will stream one event per second from the Avro file to the Kafka topic. Then, we'll create a realtime table, which
will start consuming from the Kafka topic.

.. code-block:: bash

   bin/pinot-admin.sh AddTable -filePath flights-definition-realtime.json

We can then query the table with the following query to see the events stream in:

.. code-block:: sql

   SELECT COUNT(*) FROM flights

Repeating the query multiple times should show the events slowly being streamed into the table.
