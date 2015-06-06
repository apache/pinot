# Pinot Hadoop

Introduction
------------

Pinot supports data segment generation from Hadoop.


Build
-----

To build the project:

```
mvn clean install -DskipTests
```

This will create a fat jar for pinot hadoop jar.

Run
___

Create a job properties configuration file, e.g.:

```
# Segment creation job configs:
path.to.input=pinot/input/data
path.to.output=pinot/output
path.to.schema=pinot/input/schema/data.schema
segment.table.name=testTable

# Segment tar push job configs:
push.to.hosts=controller_host_0,controller_host_1
push.to.port=8888
```

Pinot data schema file needs to be checked in locally and put the schema file in job properties file.

The `com.linkedin.pinot.hadoop.PinotHadoopJobLauncher` class (the main class of the shaded JAR in `pinot-hadoop`) should be run to accomplish this:

```
# Segment creation
    hadoop jar  pinot-hadoop-1.0-SNAPSHOT.jar SegmentCreation job.properties
  
After this point, we have built the data segment from the raw data file.
Next step is to push those data into pinot controller

# Segment tar push
    hadoop jar  pinot-hadoop-1.0-SNAPSHOT.jar SegmentTarPush job.properties

There is also a job that combines the two jobs together.

# Segment creation and tar push
    hadoop jar  pinot-hadoop-1.0-SNAPSHOT.jar SegmentCreationAndTarPush job.properties
```



