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
Pinot requires JDK 8 or later, Apache Maven 3 and `Apache Thrift<https://thrift.apache.org/docs/install/>`_.

#. Check out the code from GitHub (https://github.com/apache/incubator-pinot)
#. With Maven installed, run ``mvn install package -DskipTests -Pbin-dist -Pbuild-thrift`` in the directory in which you checked out Pinot.
#. Make the generated scripts executable ``cd pinot-distribution/target/apache-pinot-incubating-<version>-SNAPSHOT-bin; chmod +x bin/*.sh``

Trying out Offline quickstart demo
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To run the demo with compiled code:
  ``bin/quick-start-offline.sh``

Once the Pinot cluster is running, you can query it by going to http://localhost:9000/query/

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

Trying out Realtime quickstart demo
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pinot can ingest data from streaming sources such as Kafka.

To run the demo with compiled code:
  ``bin/quick-start-realtime.sh``

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

Now we have a quick start Pinot cluster running locally. The below shows a step-by-step instruction on
how to add a simple table to the Pinot system, how to upload segments, and how to query it.

Suppose we have a transcript in CSV format containing students' basic info and their scores of each subject.

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

Firstly in order to set up a table, we need to specify the schema of this transcript.

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

To upload the schema, we can use the command below:

.. code-block:: none

  $ ./pinot-distribution/target/apache-pinot-incubating-0.1.0-SNAPSHOT-bin/apache-pinot-incubating-0.1.0-SNAPSHOT-bin/bin/pinot-admin.sh AddSchema -schemaFile /Users/host1/transcript-schema.json -exec
  Executing command: AddSchema -controllerHost [controller_host] -controllerPort 9000 -schemaFilePath /Users/host1/transcript-schema.json -exec
  Sending request: http://[controller_host]:9000/schemas to controller: [controller_host], version: 0.1.0-SNAPSHOT-2c5d42a908213122ab0ad8b7ac9524fcf390e4cb

Then, we need to specify the table config which links the schema to this table:

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

And upload the table config to Pinot cluster:

.. code-block:: none

  $ ./pinot-distribution/target/apache-pinot-incubating-0.1.0-SNAPSHOT-bin/apache-pinot-incubating-0.1.0-SNAPSHOT-bin/bin/pinot-admin.sh AddTable -filePath /Users/host1/transcript-table-config.json -exec
  Executing command: AddTable -filePath /Users/host1/transcript-table-config.json -controllerHost [controller_host] -controllerPort 9000 -exec
  {"status":"Table transcript_OFFLINE successfully added"}

In order to upload our data to Pinot cluster, we need to convert our CSV file to Pinot Segment:

.. code-block:: none

  $ ./pinot-distribution/target/apache-pinot-incubating-0.1.0-SNAPSHOT-bin/apache-pinot-incubating-0.1.0-SNAPSHOT-bin/bin/pinot-admin.sh CreateSegment -dataDir /Users/host1/Desktop/test/ -format CSV -outDir /Users/host1/Desktop/test2/ -tableName transcript -segmentName transcript_0 -overwrite -schemaFile /Users/host1/transcript-schema.json
  Executing command: CreateSegment  -generatorConfigFile null -dataDir /Users/host1/Desktop/test/ -format CSV -outDir /Users/host1/Desktop/test2/ -overwrite true -tableName transcript -segmentName transcript_0 -timeColumnName null -schemaFile /Users/host1/transcript-schema.json -readerConfigFile null -enableStarTreeIndex false -starTreeIndexSpecFile null -hllSize 9 -hllColumns null -hllSuffix _hll -numThreads 1
  Accepted files: [/Users/host1/Desktop/test/Transcript.csv]
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
  Converting segment: /Users/host1/Desktop/test2/transcript_0_0 to v3 format
  v3 segment location for segment: transcript_0_0 is /Users/host1/Desktop/test2/transcript_0_0/v3
  Deleting files in v1 segment directory: /Users/host1/Desktop/test2/transcript_0_0
  Driver, record read time : 1
  Driver, stats collector time : 0
  Driver, indexing time : 0

Once we have the Pinot segment, we can upload this segment to our cluster:

.. code-block:: none

  $ ./pinot-distribution/target/apache-pinot-incubating-0.1.0-SNAPSHOT-bin/apache-pinot-incubating-0.1.0-SNAPSHOT-bin/bin/pinot-admin.sh UploadSegment -segmentDir /Users/host1/Desktop/test2/
  Executing command: UploadSegment -controllerHost [controller_host] -controllerPort 9000 -segmentDir /Users/host1/Desktop/test2/
  Compressing segment transcript_0_0
  Uploading segment transcript_0_0.tar.gz
  Sending request: http://[controller_host]:9000/v2/segments to controller: [controller_host], version: 0.1.0-SNAPSHOT-2c5d42a908213122ab0ad8b7ac9524fcf390e4cb

You made it! Now we can query the data in Pinot:

To get all the number of rows in the table:

.. code-block:: none

  $ ./pinot-distribution/target/apache-pinot-incubating-0.1.0-SNAPSHOT-bin/apache-pinot-incubating-0.1.0-SNAPSHOT-bin/bin/pinot-admin.sh PostQuery -brokerPort 8000 -query "select count(*) from transcript"
  Executing command: PostQuery -brokerHost [controller_host] -brokerPort 8000 -query select count(*) from transcript
  Result: {"aggregationResults":[{"function":"count_star","value":"4"}],"exceptions":[],"numServersQueried":1,"numServersResponded":1,"numSegmentsQueried":1,"numSegmentsProcessed":1,"numSegmentsMatched":1,"numDocsScanned":4,"numEntriesScannedInFilter":0,"numEntriesScannedPostFilter":0,"numGroupsLimitReached":false,"totalDocs":4,"timeUsedMs":7,"segmentStatistics":[],"traceInfo":{}}

To get the average score of subject Maths:

.. code-block:: none

  $ ./pinot-distribution/target/apache-pinot-incubating-0.1.0-SNAPSHOT-bin/apache-pinot-incubating-0.1.0-SNAPSHOT-bin/bin/pinot-admin.sh PostQuery -brokerPort 8000 -query "select avg(score) from transcript where subject = \"Maths\""
  Executing command: PostQuery -brokerHost [controller_host] -brokerPort 8000 -query select avg(score) from transcript where subject = "Maths"
  Result: {"aggregationResults":[{"function":"avg_score","value":"3.50000"}],"exceptions":[],"numServersQueried":1,"numServersResponded":1,"numSegmentsQueried":1,"numSegmentsProcessed":1,"numSegmentsMatched":1,"numDocsScanned":2,"numEntriesScannedInFilter":4,"numEntriesScannedPostFilter":2,"numGroupsLimitReached":false,"totalDocs":4,"timeUsedMs":33,"segmentStatistics":[],"traceInfo":{}}

To get the average score for Lucy Smith:

.. code-block:: none

  $ ./pinot-distribution/target/apache-pinot-incubating-0.1.0-SNAPSHOT-bin/apache-pinot-incubating-0.1.0-SNAPSHOT-bin/bin/pinot-admin.sh PostQuery -brokerPort 8000 -query "select avg(score) from transcript where firstName = \"Lucy\" and lastName = \"Smith\""
  Executing command: PostQuery -brokerHost [controller_host] -brokerPort 8000 -query select avg(score) from transcript where firstName = "Lucy" and lastName = "Smith"
  Result: {"aggregationResults":[{"function":"avg_score","value":"3.65000"}],"exceptions":[],"numServersQueried":1,"numServersResponded":1,"numSegmentsQueried":1,"numSegmentsProcessed":1,"numSegmentsMatched":1,"numDocsScanned":2,"numEntriesScannedInFilter":6,"numEntriesScannedPostFilter":2,"numGroupsLimitReached":false,"totalDocs":4,"timeUsedMs":67,"segmentStatistics":[],"traceInfo":{}}
