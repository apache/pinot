<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Pinot Spark

Introduction
------------

Pinot supports data segment generation from Spark.


Build
-----

To build the project:

```
mvn clean install -DskipTests -Pbuild-shaded-jar
```

This will create a fat jar for pinot spark jar.

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

The `org.apache.pinot.spark.PinotSparkJobLauncher` class (the main class of the shaded JAR in `pinot-spark`) should be run to accomplish this:

```
# Segment creation
    spark-submit \
      --class org.apache.pinot.spark.PinotSparkJobLauncher \
      --master <master-url> \
      --deploy-mode <deploy-mode> \
      pinot-spark-0.2.0-SNAPSHOT-shaded.jar \
      SegmentCreation job.properties
  
After this point, we have built the data segment from the raw data file.
Next step is to push those data into pinot controller

# Segment tar push
    spark-submit \
      --class org.apache.pinot.spark.PinotSparkJobLauncher \
      --master <master-url> \
      --deploy-mode <deploy-mode> \
      pinot-spark-0.2.0-SNAPSHOT-shaded.jar \
      SegmentTarPush job.properties

There is also a job that combines the two jobs together.

# Segment creation and tar push
    spark-submit \
      --class org.apache.pinot.spark.PinotSparkJobLauncher \
      --master <master-url> \
      --deploy-mode <deploy-mode> \
      pinot-spark-0.2.0-SNAPSHOT-shaded.jar \
      SegmentCreationAndTarPush job.properties
```



