Running the Pinot Demonstration
===============================

A quick way to get familiar with Pinot is to run the Pinot examples. The examples can be run either by compiling the
code or by running the prepackaged Docker images.

Basic Pinot querying
--------------------

To demonstrate Pinot, let's start a simple one node cluster, along with the required Zookeeper. This demo setup also
creates a table, generates some Pinot segments, uploads them to the cluster and makes them queryable.

All of the setup is automated, so the only thing required at the beginning is to start the demonstration cluster.

Running Pinot using Docker
~~~~~~~~~~~~~~~~~~~~~~~~~~

With Docker installed, the Pinot demonstration can be run using ``docker run -it -p 9000:9000
linkedin/pinot-quickstart-offline``. This will start a single node Pinot cluster with a preloaded data set.

Running Pinot by compiling the code
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One can also run the Pinot demonstration by checking out the code on GitHub, compiling it, and running it. Compiling Pinot requires JDK 8 or later and Apache Maven 3.

#. Check out the code from GitHub (https://github.com/linkedin/pinot)
#. With Maven installed, run ``mvn install package -DskipTests`` in the directory in which you checked out Pinot.
#. Make the generated scripts executable ``cd pinot-distribution/target/pinot-0.016-pkg; chmod +x bin/*.sh``
#. Run Pinot: ``bin/quick-start-offline.sh``

Trying out the demo
~~~~~~~~~~~~~~~~~~~

Once the Pinot cluster is running, you can query it by going to http://localhost:9000/query/

You can also use the REST API to query Pinot, as well as the Java client. (TODO link to the REST API and Java client docs)

Pinot uses PQL, a SQL-like query language, to query data. Here are some sample queries. (TODO link to the PQL documentation)

Realtime data ingestion
-----------------------
