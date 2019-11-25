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

## Build and Usage
To build the project:

```
mvn clean install -DskipTests
```


This will create `druid-to-pinot-schema-generator-jar-with-dependencies.jar` and 
`druid-to-pinot-segment-converter-jar-with-dependencies.jar` inside the `target` directory.

A Pinot schema is required to run the `segment converter` command, which can either be written and provided by the user or
generated from the Druid segment with the `schema generator` command.

The `schema generator` command is used as follows*:

```
java -jar druid-to-pinot-schema-generator-jar-with-dependencies.jar <schema_name> \
<druid_segment_path> \
<schema_output_path>
```

Keep in mind that a Druid segment is a directory rather than a single file.

The `segment converter` command is used as follows*:

```
java -jar druid-to-pinot-segment-converter-jar-with-dependencies.jar \
<pinot_table_name> \
<pinot_segment_name> \
<pinot_schema_path> \
<druid_segment_path> \
<segment_output_path>
```

*Backslashes and newlines are used for readability and ease of use.