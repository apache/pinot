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

# Generating and Loading Synthetic Data

Mock data has many use-cases from testing over benchmarking to portable application demos. The generator configs in this
directory produce neat synthetic time series data of an imaginary website. You can generate gigabytes of mock data with
these patterns if you so desire.

**simpleWebsite** generates non-dimensional data with views, clicks, and error count metrics

**complexWebsite** generates similar metrics with a 3-dimensional breakdown across countries, browsers, and platforms

The command line examples below are meant to be executed from the **pinot repository root**.
(This was tested with pinot-quickstart in batch mode. Requires DefaultTenant and broker)

## Generate data via pattern

This first step generates the raw data from a given generator file. By default, we generate the data as CSV, and you can
have a look manually with your favorite spreadsheet tool.

(may require **rm -rf ./myTestData** to clear out existing mock data)

```
./pinot-tools/target/pinot-tools-pkg/bin/pinot-admin.sh GenerateData \
-numFiles 1 -numRecords 354780  -format csv \
-schemaFile ./pinot-tools/src/main/resources/generator/complexWebsite_schema.json \
-schemaAnnotationFile ./pinot-tools/src/main/resources/generator/complexWebsite_generator.json \
-outDir ./myTestData
```

## Generate Pinot Segment

Now we turn the verbose CSV data into an efficiently packed segment ready for upload into pinot.

```
./pinot-tools/target/pinot-tools-pkg/bin/pinot-admin.sh CreateSegment \
-tableConfigFile ./pinot-tools/src/main/resources/generator/complexWebsite_config.json \
-format CSV -overwrite \
-schemaFile ./pinot-tools/src/main/resources/generator/complexWebsite_schema.json \
-dataDir ./myTestData \
-outDir ./myTestSegment 
```

## Create Pinot Table

Before we push the segment, let's ensure that we have a table namespace ready. You can skip this step if you created a
table earlier already.

```
./pinot-tools/target/pinot-tools-pkg/bin/pinot-admin.sh AddTable -exec \
-tableConfigFile ./pinot-tools/src/main/resources/generator/complexWebsite_config.json \
-schemaFile ./pinot-tools/src/main/resources/generator/complexWebsite_schema.json
```

## Upload Pinot Segment

Now, we upload the segment. After this step, data should be available and query-able from the pinot console an any
connected applications.

```
./pinot-tools/target/pinot-tools-pkg/bin/pinot-admin.sh UploadSegment \
-tableName complexWebsite \
-segmentDir ./myTestSegment
```

## Check data availability

We can finally check data availability, e.g. by using pinot's built-in query console. If you're running a local
pinot-quickstart image via docker the URL should be:

```
http://localhost:9000#
```
