Running the Pinot Demonstration
===============================

A quick way to get familiar with Pinot is to run the Pinot examples. The examples can be run either by compiling the
code or by running the prepackaged Docker images.

Basic Pinot querying
--------------------

To demonstrate Pinot, let's start a simple one node cluster, along with the required Zookeeper. This demo setup also
creates a table, generates some Pinot segments, then uploads them to the cluster in order to make them queryable.

All of the setup is automated, so the only thing required at the beginning is to start the demonstration cluster.

Running Pinot using Docker
~~~~~~~~~~~~~~~~~~~~~~~~~~

With Docker installed, the Pinot demonstration can be run using ``docker run -it -p 9000:9000
linkedin/pinot-quickstart-offline``. This will start a single node Pinot cluster with a preloaded data set.

Running Pinot by compiling the code
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One can also run the Pinot demonstration by checking out the code on GitHub, compiling it, and running it. Compiling
Pinot requires JDK 8 or later and Apache Maven 3.

#. Check out the code from GitHub (https://github.com/apache/incubator-pinot)
#. With Maven installed, run ``mvn install package -DskipTests`` in the directory in which you checked out Pinot.
#. Make the generated scripts executable ``cd pinot-distribution/target/pinot-0.016-pkg; chmod +x bin/*.sh``
#. Run Pinot: ``bin/quick-start-offline.sh``

Trying out the demo
~~~~~~~~~~~~~~~~~~~

Once the Pinot cluster is running, you can query it by going to http://localhost:9000/query/

You can also use the REST API to query Pinot, as well as the Java client. As this is outside of the scope of this
introduction, the reference documentation to use the Pinot client APIs is in the :ref:`client-api` section.

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

Realtime data ingestion
-----------------------

Pinot can also ingest data from streaming sources, such as Kafka streams. To run the realtime data ingestion demo, you
can run ``docker run -it -p 9000:9000 linkedin/pinot-quickstart-realtime`` using Docker, or
``bin/quick-start-realtime.sh`` if using the self-compiled version.

Once started, the demo will start Kafka, create a Kafka topic, and create a realtime Pinot table. Once created, Pinot
will start ingesting events from the Kafka topic into the table. The demo also starts a consumer that consumes events
from the Meetup API and pushes them into the Kafka topic that was created, causing new events modified on Meetup to
show up in Pinot.

.. role:: sql(code)
  :language: sql

To show new events appearing, one can run :sql:`SELECT * FROM meetupRsvp ORDER BY mtime DESC LIMIT 50` repeatedly, which shows the
last events that were ingested by Pinot.

