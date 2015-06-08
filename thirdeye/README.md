ThirdEye
========

A system for efficient monitoring of and drill-down into business metrics.

Introduction
------------

Consider the problem of computing aggregates on a set of data. There are two
extremes in terms of complexity with respect to space and time: 

* Pre-materialize no aggregates, and require a scan of the data at runtime
* Or, pre-materialize all of the aggregates, and require a simple key/value lookup at runtime

The former optimizes for space, whereas the latter optimizes for time.

ThirdEye attempts to occupy a sweet-spot between these two extremes by
pre-materializing a subset of aggregates with the goal to bound the number of
records that need to be scanned to answer any given aggregation query.

Build
-----

To build the project:

```
./build
```

Configuration
-------------

To configure ThirdEye, one must minimally specify

* Dimension field names
* Metric field names (and types)
* Time field name (granuarity and retention)

In addition to this, one can specify a custom rollup function (used in bootstrap)
to obtain a form of [iceberg cubing](http://www2.cs.uregina.ca/~dbd/cs831/notes/dcubes/iceberg.html).

Bootstrap
---------

To generate ThirdEye data, we use Hadoop to process raw Avro data.

Before running the job, ensure the following directory structure and files
exist on HDFS:

```
{rootDir}/
  {collection}/
    config.yml    # your collection configuration 
    schema.avsc   # the schema for your raw Avro data
```

After this exists, create a job properties configuration file, e.g.:

```
thirdeye.root=thirdeye
thirdeye.collection=abook
input.paths=thirdeye-input/abook
```

The job will scan for avro files recursively in `input.paths`.

First, we must generate the star tree data structure. We want to do this on a
big enough sample of data such that we capture the majority of periodicity in
the data. Also, this needs to be done relatively infrequently, as we can re-use
the star tree on subsequent data.

The `com.linkedin.thirdeye.bootstrap.ThirdEyeJob` class (the main class of the
shaded JAR in `thirdeye-bootstrap`) should be run to accomplish this:

```
# Analyze the input data
hadoop jar thirdeye-bootstrap-1.0-SNAPSHOT.jar analysis job.properties

# Aggregate at the granularity specified for ThirdEye
hadoop jar thirdeye-bootstrap-1.0-SNAPSHOT.jar aggregation job.properties

# Splits input data into above / below threshold using function
hadoop jar thirdeye-bootstrap-1.0-SNAPSHOT.jar rollup_phase1 job.properties

# Aggregates all possible combinations of raw dimension combination below threshold
hadoop jar thirdeye-bootstrap-1.0-SNAPSHOT.jar rollup_phase2 job.properties

# Selects the rolled-up dimension key for each raw dimension combination
hadoop jar thirdeye-bootstrap-1.0-SNAPSHOT.jar rollup_phase3 job.properties

# Sums metric time series by the rolled-up dimension key
hadoop jar thirdeye-bootstrap-1.0-SNAPSHOT.jar rollup_phase4 job.properties

# Builds star tree index structure using rolled-up dimension combination and those above threshold
hadoop jar thirdeye-bootstrap-1.0-SNAPSHOT.jar startree_generation job.properties
```

After this point, we have built the star tree data structure, but we haven't loaded any data yet. We now perform the following two steps to load the data:

```
# Sums raw Avro time-series data by dimension key
hadoop jar thirdeye-bootstrap-1.0-SNAPSHOT.jar startree_bootstrap_phase1 job.properties

# Groups records by star tree leaf node and creates leaf buffers
hadoop jar thirdeye-bootstrap-1.0-SNAPSHOT.jar startree_bootstrap_phase2 job.properties
```

_Note: for incremental updates, the `analysis` phase must be run before `startree_bootstrap_phase1`_

Load
----

To load data _the first time_ from HDFS into a local directory, use the
`DataLoadTool` available via `thirdeye-tools` shaded JAR, e.g.:

```
# Get config, star-tree, dimension stores for abook from app user's thirdeye root directory
java -jar thirdeye-tools/target/thirdeye-tools-1.0-SNAPSHOT-shaded.jar DataLoadTool \
  -krb5 ~/Desktop/krb5.conf \
  -includeConfig \
  -includeStarTree \
  -includeDimensions \
  http://hdfs-namenode-machine:50070/user/app/thirdeye
  file:///tmp/thirdeye \
  abook
```

Then to pull only metrics from incremental uploads

```
java -jar thirdeye-tools/target/thirdeye-tools-1.0-SNAPSHOT-shaded.jar DataLoadTool \
  -krb5 ~/Desktop/krb5.conf \
  http://hdfs-namenode-machine:50070/user/app/thirdeye
  file:///tmp/thirdeye \
  abook
```

One can also use the `-minTime` and `-maxTime` CLI arguments to control which
segments are downloaded.

This tool uses Kerberos for WebHDFS authentication. For more information on
krb5.conf file, please read
[this](http://web.mit.edu/kerberos/krb5-1.5/krb5-1.5/doc/krb5-admin/krb5.conf.html).

_TODO: Add REST endpoint to upload data_

Serve
-----

A sample server config:

```
rootDir: /tmp/thirdeye
autoRestore: true
```

To run the server:

```
java -jar thirdeye-server/target/thirdeye-server-1.0-SNAPSHOT-shaded.jar server /tmp/server.yml
```

_For more information on server configuration, see [dropwizard.io](http://dropwizard.io/)_
