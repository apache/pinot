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

This tool takes a local Druid segment and uses it to generate a corresponding Pinot segment with all of the columns specified
in a given Pinot schema and Pinot table config.

## Build and Usage
To build the project:

```
mvn clean install -DskipTests
```

This will create `pinot-druid-migration-jar-with-dependencies.jar` inside the `target` directory, which can be used 
as follows:
```
java -jar pinot-druid-migration-jar-with-dependencies.jar <pinot_table_name> <pinot_segment_name> <pinot_schema_path> <druid_segment_path> <output_path>
```