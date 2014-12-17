
For making review board a little more easy, you can add the following in your ~/.gitconfig,
if you dont have a ~/.gitconfig then create one and then add 

[linkedin]
        publish = true
        groups = pinot-dev-reviewers
        reviewers = dpatel,xiafu,kgopalak,jfim





# Pinot

A realtime OLAP datastore.

## Install

Both JDK version 7 or greater and maven are required. Building the Pinot artifacts can be done using 

    mvn -DskipTests=true install

## Running

Pinot can be started using

    cd pinot-server
    mvn exec:java -Dexec.mainClass=com.linkedin.pinot.server.starter.SingleNodeServerStarter -Dexec.args="-server_conf=src/test/resources/conf"

This will start an empty standalone instance. TODO Add more text here

## Configuration

Pinot is configured using two files: pinot.properties and pinot-broker.properties. TODO

### Standalone Mode

For testing purposes, Pinot can be ran in standalone mode.

    TODO: The command

### Distributed Mode

Pinot is designed to be ran in distributed mode. Starting a Pinot cluster requires a ZooKeeper cluster TODO

## Building Indices

Indices are built on Hadoop using the supplied MapReduce jobs. An example faceted counter job TODO

    TODO: hadoop jar pinot-hadoop.jar todo.todo.Todo /some/input/dir

This builds the indices, which can then be uploaded to a Pinot node by doing

    TODO: curl blah blah blah

## Realtime Indexing

Pinot supports realtime indexing by reading Avro-formatted data directly from Kafka. This does not replace the
Hadoop-based indexing, but is rather used to provide realtime updates on top of a data source that is also persisted
into HDFS. When an index segment is built from Hadoop, it will replace the equivalent segment that has been built from
realtime data.

An example of realtime indexing can be done by starting Kafka (see sample Kafka config TODO), then configuring Pinot to
read from a specified topic by changing the Pinot configuration file as below

    TODO: Configuration file example

Then, events pushed into Kafka will be consumed by Pinot. A sample producer that works with the above configuration can
be started by running

    TODO: java -jar blah-blah.jar

This will push randomly generated events in Kafka, which can then be queried.

## Querying

Pinot is queried using the Pinot Query Language (PQL). TODO

    TODO: SELECT blah, COUNT(*) FROM someTable WHERE blah < 10
