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

.. warning::  The documentation is not up-to-date and has moved to `Apache Pinot Docs <https://docs.pinot.apache.org/>`_.

.. _getting-started:

Getting Started
===============

A quick way to get familiar with Pinot is to run the Pinot examples. The examples can be run either by compiling the
code or by running the prepackaged Docker images.

To demonstrate Pinot, let's start a simple one node cluster, along with the required Zookeeper. This demo setup also
creates a table, generates some Pinot segments, then uploads them to the cluster in order to make them queryable.

All of the setup is automated, so the only thing required at the beginning is to start the demonstration cluster.


.. _compiling-code-section:

Compiling the code
~~~~~~~~~~~~~~~~~~

One can also run the Pinot demonstration by checking out the code on GitHub, compiling it, and running it. Compiling
Pinot requires JDK 8 or later and Apache Maven 3.

#. Check out the code from GitHub (https://github.com/apache/incubator-pinot)
#. With Maven installed, run ``mvn install package -DskipTests -Pbin-dist`` in the directory in which you checked out Pinot.
#. Make the generated scripts executable:

.. code-block:: none

  cd pinot-distribution/target/apache-pinot-incubating-<version>-SNAPSHOT-bin/apache-pinot-incubating-<version>-SNAPSHOT-bin; chmod +x bin/*.sh

Trying out Batch quickstart demo
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To run the demo with compiled code:
  ``bin/quick-start-batch.sh``

Once the Pinot cluster is running, you can query it by going to http://localhost:9000

You can also use the REST API to query Pinot, as well as the Java client. As this is outside of the scope of this
introduction, the reference documentation to use the Pinot client APIs is in the :doc:`client_api` section.

Pinot uses PQL, a SQL-like query language, to query data. Here are some sample queries:

.. code-block:: sql

  /*Total number of documents in the table*/
  SELECT count(*) FROM baseballStats LIMIT 0

  /*Top 5 run scorers of all time*/
  SELECT sum('runs') FROM baseballStats GROUP BY playerName TOP 5 LIMIT 0

  /*Top 5 run scorers of the year 2000*/
  SELECT sum('runs') FROM baseballStats WHERE yearID=2000 GROUP BY playerName TOP 5 LIMIT 0

  /*Top 10 run scorers after 2000*/
  SELECT sum('runs') FROM baseballStats WHERE yearID>=2000 GROUP BY playerName

  /*Select playerName,runs,homeRuns for 10 records from the table and order them by yearID*/
  SELECT playerName,runs,homeRuns FROM baseballStats ORDER BY yearID LIMIT 10

The full reference for the PQL query language is present in the :ref:`pql` section of the Pinot documentation.

Trying out Streaming quickstart demo
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pinot can ingest data from streaming sources such as Kafka.

To run the demo with compiled code:
  ``bin/quick-start-streaming.sh``

Once started, the demo will start Kafka, create a Kafka topic, and create a realtime Pinot table. Once created, Pinot
will start ingesting events from the Kafka topic into the table. The demo also starts a consumer that consumes events
from the Meetup API and pushes them into the Kafka topic that was created, causing new events modified on Meetup to
show up in Pinot.

.. role:: sql(code)
  :language: sql

To show new events appearing, one can run :sql:`SELECT * FROM meetupRsvp ORDER BY mtime DESC LIMIT 50` repeatedly, which shows the
last events that were ingested by Pinot.

Experimenting with Pinot
~~~~~~~~~~~~~~~~~~~~~~~~

Now we have a quick start Pinot cluster running locally. Below are step-by-step instructions on
how to add a simple table to the Pinot system, how to upload a segment, and how to query the segment.

Suppose we have a transcript in CSV format containing students' basic info and their scores for each subject.

+------------+------------+-----------+-----------+-----------+-----------+
| studentID  | firstName  | lastName  |   gender  |  subject  |   score   |
+============+============+===========+===========+===========+===========+
|     200    |     Lucy   |   Smith   |   Female  |   Maths   |    3.8    |
+------------+------------+-----------+-----------+-----------+-----------+
|     200    |     Lucy   |   Smith   |   Female  |  English  |    3.5    |
+------------+------------+-----------+-----------+-----------+-----------+
|     201    |     Bob    |    King   |    Male   |   Maths   |    3.2    |
+------------+------------+-----------+-----------+-----------+-----------+
|     202    |     Nick   |   Young   |    Male   |  Physics  |    3.6    |
+------------+------------+-----------+-----------+-----------+-----------+

When we create a CSV file, we will also need a separate CSV config JSON file.

First, however, we will create a working directory called ``getting-started`` (in this example, it is on ``Desktop``), and create two additional directories within ``getting-started`` called ``data``
and ``config``.

Note that we can create a variable for the working directory called ``WORKING_DIR``.

.. code-block:: none

  $ mkdir getting-started
  $ WORKING_DIR=/Users/host1/Desktop/getting-started
  $ cd $WORKING_DIR
  $ mkdir getting-started/data
  $ mkdir getting started/config

We will create the transcript CSV file in ``data``, and the CSV config file in ``config``.

.. code-block:: none

  $ touch getting-started/data/test.csv
  $ touch getting-started/config/csv-record-reader-config.json

The ``test.csv`` file should look like this, with no header line at the top:

.. code-block:: none

  200,Lucy,Smith,Female,Maths,3.8
  200,Lucy,Smith,Female,English,3.5
  201,Bob,King,Male,Maths,3.2
  202,Nick,Young,Male,Physics,3.6

Instead of using a header line, we will use the CSV config JSON file ``csv-record-reader-config.json`` to specify the header:

.. code-block:: none

  {
    "header":"studentID,firstName,lastName,gender,subject,score",
    "fileFormat":"CSV"
  }

In order to set up a table, we need to specify the schema of this transcript in ``transcript-schema.json``, which we will store in ``config``:

.. code-block:: none

  $ touch getting-started/config/transcript-schema.json

``transcript-schema.json`` should look like this:

.. code-block:: none

  {
    "schemaName": "transcript",
    "dimensionFieldSpecs": [
      {
        "name": "studentID",
        "dataType": "STRING"
      },
      {
        "name": "firstName",
        "dataType": "STRING"
      },
      {
        "name": "lastName",
        "dataType": "STRING"
      },
      {
        "name": "gender",
        "dataType": "STRING"
      },
      {
        "name": "subject",
        "dataType": "STRING"
      }
    ],
    "metricFieldSpecs": [
      {
        "name": "score",
        "dataType": "FLOAT"
      }
    ]
  }

Then, we need to specify the table config in another JSON file (also stored in ``config``), which links the schema to the table:

.. code-block:: none

  $ touch getting-started/config/transcript-table-config.json

``transcript-table-config.json`` should look like this:

.. code-block:: none

  {
    "tableName": "transcript",
    "segmentsConfig" : {
      "replication" : "1",
      "schemaName" : "transcript",
      "segmentAssignmentStrategy" : "BalanceNumSegmentAssignmentStrategy"
    },
    "tenants" : {
      "broker":"DefaultTenant",
      "server":"DefaultTenant"
    },
    "tableIndexConfig" : {
      "invertedIndexColumns" : [],
      "loadMode"  : "HEAP",
      "lazyLoad"  : "false"
    },
    "tableType":"OFFLINE",
    "metadata": {}
  }


To create pinot table, we can navigate to the directory in ``pinot-distribution`` that contains
``pinot-admin.sh``, and use the command below:

.. code-block:: none

  $ ./pinot-admin.sh AddTable -schemaFile $WORKING_DIR/config/transcript-schema.json -tableConfigFile $WORKING_DIR/config/transcript-table-config.json -exec
  Executing command: AddTable -tableConfigFile /Users/host1/Desktop/getting-started/config/transcript-table-config.json -schemaFile /Users/host1/Desktop/getting-started/config/transcript-schema.json -controllerHost [controller_host] -controllerPort 9000 -exec
  {"status":"Table transcript_OFFLINE successfully added"}

At this point, the directory tree for our ``getting-started`` should look like this:

.. code-block:: none

  |-- getting-started
      |-- data
             |-- test.csv
      |-- config
             |-- csv-record-reader-config.json
             |-- transcript-schema.json
             |-- transcript-table-config.json


In order to upload our data to the Pinot cluster, we need to convert our CSV file into a Pinot Segment, which will be put in a new directory $WORKING_DIR/test2:

.. code-block:: none

  $ ./pinot-admin.sh CreateSegment -dataDir $WORKING_DIR/data -format CSV -outDir $WORKING_DIR/test2 -tableName transcript -segmentName transcript_0 -overwrite -schemaFile $WORKING_DIR/config/transcript-schema.json -readerConfigFile $WORKING_DIR/config/csv-record-reader-config.json
  Executing command: CreateSegment  -generatorConfigFile null -dataDir /Users/host1/Desktop/getting-started/data -format CSV -outDir /Users/host1/Desktop/getting-started/test2 -overwrite true -tableName transcript -segmentName transcript_0 -timeColumnName null -schemaFile /Users/host1/Desktop/getting-started/config/transcript-schema.json -readerConfigFile /Users/host1/Desktop/getting-started/config/csv-record-reader-config.json -enableStarTreeIndex false -starTreeIndexSpecFile null -hllSize 9 -hllColumns null -hllSuffix _hll -numThreads 1
  Accepted files: [file:/Users/host1/Desktop/getting-started/data/test.csv]
  Finished building StatsCollector!
  Collected stats for 4 documents
  Created dictionary for STRING column: studentID with cardinality: 1, max length in bytes: 4, range: null to null
  Created dictionary for STRING column: firstName with cardinality: 3, max length in bytes: 4, range: Bob to Nick
  Created dictionary for STRING column: lastName with cardinality: 3, max length in bytes: 5, range: King to Young
  Created dictionary for FLOAT column: score with cardinality: 4, range: 3.2 to 3.8
  Created dictionary for STRING column: gender with cardinality: 2, max length in bytes: 6, range: Female to Male
  Created dictionary for STRING column: subject with cardinality: 3, max length in bytes: 7, range: English to Physics
  Start building IndexCreator!
  Finished records indexing in IndexCreator!
  Finished segment seal!
  Converting segment: /Users/host1/Desktop/getting-started/test2/transcript_0_0 to v3 format
  v3 segment location for segment: transcript_0_0 is /Users/host1/Desktop/getting-started/test2/transcript_0_0/v3
  Deleting files in v1 segment directory: /Users/host1/Desktop/getting-started/test2/transcript_0_0
  Driver, record read time : 1
  Driver, stats collector time : 0
  Driver, indexing time : 0

Once we have the Pinot Segment, we can upload it to our cluster:

.. code-block:: none

  $ ./pinot-admin.sh UploadSegment -segmentDir $WORKING_DIR/test2/
  Executing command: UploadSegment -controllerHost [controller_host] -controllerPort 9000 -segmentDir /Users/host1/Desktop/test2/
  Compressing segment transcript_0_0
  Uploading segment transcript_0_0.tar.gz
  Sending request: http://[controller_host]:9000/v2/segments to controller: [controller_host], version: 0.2.0-SNAPSHOT-68092ab9eb83af173d725ec685c22ba4eb5bacf9

You did it! Now we can query the data in Pinot.

To get all the number of rows in the table:

.. code-block:: none

  $ ./pinot-admin.sh PostQuery -brokerPort 8000 -query "select count(*) from transcript"
  Executing command: PostQuery -brokerHost [controller_host] -brokerPort 8000 -query select count(*) from transcript
  Result: {"aggregationResults":[{"function":"count_star","value":"4"}],"exceptions":[],"numServersQueried":1,"numServersResponded":1,"numSegmentsQueried":1,"numSegmentsProcessed":1,"numSegmentsMatched":1,"numDocsScanned":4,"numEntriesScannedInFilter":0,"numEntriesScannedPostFilter":0,"numGroupsLimitReached":false,"totalDocs":4,"timeUsedMs":7,"segmentStatistics":[],"traceInfo":{}}

To get the average score of subject Maths:

.. code-block:: none

  $ ./pinot-admin.sh PostQuery -brokerPort 8000 -query "select avg(score) from transcript where subject = \"Maths\""
  Executing command: PostQuery -brokerHost [controller_host] -brokerPort 8000 -query select avg(score) from transcript where subject = "Maths"
  Result: {"aggregationResults":[{"function":"avg_score","value":"3.50000"}],"exceptions":[],"numServersQueried":1,"numServersResponded":1,"numSegmentsQueried":1,"numSegmentsProcessed":1,"numSegmentsMatched":1,"numDocsScanned":2,"numEntriesScannedInFilter":4,"numEntriesScannedPostFilter":2,"numGroupsLimitReached":false,"totalDocs":4,"timeUsedMs":33,"segmentStatistics":[],"traceInfo":{}}

To get the average score for Lucy Smith:

.. code-block:: none

  $ ./pinot-admin.sh PostQuery -brokerPort 8000 -query "select avg(score) from transcript where firstName = \"Lucy\" and lastName = \"Smith\""
  Executing command: PostQuery -brokerHost [controller_host] -brokerPort 8000 -query select avg(score) from transcript where firstName = "Lucy" and lastName = "Smith"
  Result: {"aggregationResults":[{"function":"avg_score","value":"3.65000"}],"exceptions":[],"numServersQueried":1,"numServersResponded":1,"numSegmentsQueried":1,"numSegmentsProcessed":1,"numSegmentsMatched":1,"numDocsScanned":2,"numEntriesScannedInFilter":6,"numEntriesScannedPostFilter":2,"numGroupsLimitReached":false,"totalDocs":4,"timeUsedMs":67,"segmentStatistics":[],"traceInfo":{}}
