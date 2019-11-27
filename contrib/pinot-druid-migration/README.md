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
# Druid to Pinot Segment Converter Tool

This tool takes a local Druid segment and uses it to generate a corresponding Pinot segment with all of the columns 
specified in a given Pinot schema.

## Build
To build the project:

```
mvn clean install -DskipTests
```

This will create `druid-to-pinot-migration-tool-jar-with-dependencies.jar` inside the `target` directory.

##Commands

###ConvertSchema

In order to convert Druid segments, a Pinot schema is required, which can either be written and provided by the user or
generated from the Druid segment with the `ConvertSchema` command.

The `ConvertSchema` command is used as follows*:

```
java -jar druid-to-pinot-schema-generator-jar-with-dependencies.jar ConvertSchema \
-ingestionSpec <path_to_druid_ingestion_spec> \
-outputPath <pinot_schema_output_path>
```

###ConvertSegment

Keep in mind that a Druid segment is a directory rather than a single file.

The `ConvertSegment` command is used as follows*:

```
java -jar druid-to-pinot-segment-converter-jar-with-dependencies.jar ConvertSegment \
-pinotTableName <pinot_table_name> \
-pinotSegmentName <pinot_segment_name> \
-pinotSchemaPath <pinot_schema_path> \
-druidSegmentPath <druid_segment_path> \
-outputPath <segment_output_path>
```

###ConvertSegmentHadoop

The `ConvertSegmentHadoop` command converts a Druid segment into a Pinot segment and automatically pushes the resulting
Pinot segment to the specified Pinot cluster.

The Hadoop job for converting segments can only validate data in single files, and will do a recursive search for single
data files if a directory path is given. Since a Druid segment is a directory containing all its multiple file components
(`meta.smoosh`, `version.bin`, etc.), the Druid segment directory must be compressed into a `tar.gz` file. This can be 
done with the following command:

```
tar -czvf name-of-newly-compressed-directory.tar.gz /path/to/directory-to-be-compressed
```

The `ConvertSegmentHadoop` command also requires a job properties configuration file, such as one below:

```
# path.to.input: Input path to the compressed Druid segment, or a directory containing multiple compressed Druid segments.
path.to.input=/user/druid/segment/input/data.tar.gz

# path.to.output: Output directory for the resulting Pinot segment
path.to.output=/user/druid/segment/output

# path.to.schema: Pinot schema file for the table, stored locally
path.to.schema=/user/pinot/schema.json

# segment.table.name: Name of the table for which to generate segments
segment.table.name=segment_name

# === Segment tar push job config ===

# push.to.hosts: Controller host name to which to push
push.to.hosts=localhost

# push.to.port: The port on which the controller runs
push.to.port=9000

```

Before running this command, please also make sure that the Pinot cluster is running on the host and port specified in 
the job properties file.

Finally, the  `ConvertSegmentHadoop` command is used as follows*:

```
java -jar druid-to-pinot-migration-tool-jar-with-dependencies.jar ConvertSegmentHadoop \
-jobProperties <path_to_job_properties_file>
```


*Backslashes and newlines are used for readability and ease of use; These commands can also be written all on one line.