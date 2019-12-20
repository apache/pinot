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
# Druid to Pinot Segment Converter

This project, which can run both locally and on Hadoop, takes a Druid segment and uses it to generate a corresponding 
Pinot segment.

##ConvertSegment Command Line Tool

### Build
To build the project:

```
mvn clean install -DskipTests
```

###Usage

This will create `druid-to-pinot-migration-tool-jar-with-dependencies.jar` inside the `target` directory.

Keep in mind that a Druid segment is comprised of multiple file components (`meta.smoosh`, `version.bin`, etc.) rather 
than a single file. This tool works with both Druid segment directories and compressed Druid segment files (`.zip` and 
`.tar.gz` files).

The `ConvertSegment` command is used as follows*.

The `-pinotSchemaPath` is optional; If it is not specified, the tool will generate a Pinot schema from the given Druid
segment.

```
java -jar druid-to-pinot-segment-converter-jar-with-dependencies.jar ConvertSegment \
-pinotTableName <pinot_table_name> \
-pinotSegmentName <pinot_segment_name> \
-pinotSchemaPath <pinot_schema_path> \
-pinotTableConfigPath <pinot_table_config_path> \
-druidSegmentPath <druid_segment_path> \
-outputPath <segment_output_path>
```

*Backslashes and newlines are used for readability and ease of use; These commands can also be written all on one line.

##Segment Converter Hadoop Job

The `SegmentConverterHadoopJob` converts Druid segments into Pinot segments on Hadoop.

The Hadoop job for converting segments can only validate data in single files, and will do a recursive search for single
data files if a directory path is given. 

Since a Druid segment is a collection of multiple file components, the Druid segment directory must be compressed into a
 `.zip` or `.tar.gz` file. 


Druid segments in storage are typically already compressed, but compression to a `.tar.gz` file can be done with the 
following command:

```
$ tar -czvf name-of-newly-compressed-directory.tar.gz /path/to/directory-to-be-compressed
```

The `ConvertSegmentHadoop` command also requires a job properties configuration file, such as one below:

```
# path.to.input: Input Hadoop path to the compressed Druid segment, or a directory containing multiple compressed Druid segments.
path.to.input=/user/druid/segment/input/data.tar.gz

# path.to.output: Output Hadoop directory for the resulting Pinot segment
path.to.output=/user/druid/segment/output

# path.to.schema: Pinot schema file for the table, stored locally
path.to.schema=/User/pinot/schema.json

# segment.table.name: Name of the table for which to generate segments
segment.table.name=segment_name

# === Segment tar push job config ===

# push.to.hosts: Controller host name to which to push
push.to.hosts=localhost

# push.to.port: The port on which the controller runs
push.to.port=9000

```

****Before running this command, please also make sure that the Pinot cluster is running on the host and port specified in 
the job properties file, _AND_ make sure that a Pinot TableConfig has already been pushed to the cluster.**

Finally, the  `ConvertSegmentHadoop` command is built and used as follows*:

####Build

The segment converter for Hadoop is built separately from the command line tool.

```
mvn clean install -DskipTests -Pbuild-shaded-jar
```

This will create `druid-to-pinot-migration-HADOOP-JOB.jar` inside the `target` directory.

####Usage

After building the tool, please run the following commands to ensure that `META-INF/license` and  `license` will not
cause any issues:

```
zip -d druid-to-pinot-migration-HADOOP-JOB.jar META-INF/license/*
zip -d druid-to-pinot-migration-HADOOP-JOB.jar license/*

```

Finally, with Hadoop running, the jar can be executed as follows:

```
hadoop jar druid-to-pinot-migration-HADOOP-JOB.jar job.properties
```

Once the Pinot segment has been created and moved to the output directory, you can use the 
Pinot SegmentTarPush command located in `pinot-hadoop` to push the segment to the cluster using the
same job properties file.