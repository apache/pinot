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
# TPC-H integration tests

`TPCHQueryIntegrationTest` runs the 22 TPC-H queries on the multi-stage engine and compares the results with H2.
The data and the queries are in `pinot-tools/src/main/resources/examples/batch/tpch`, the same files that the TPC-H
quickstart uses. See `queries/README.md` in that folder for the two forms of each query and the table layout that the
rewritten queries need.

## Data

The data is the TPC-H data at scale factor 0.01 (60,175 `lineitem` rows), generated with `dbgen` from the TPC-H
tools, version 3.0.1. Pinot stores the dates as strings.

To generate the data again:

1. Build `dbgen` from the TPC-H tools. If the build fails with `fatal error: 'malloc.h' file not found`, replace
   `malloc.h` with `stdlib.h` in the files that include it.
2. Generate the `.tbl` files:
   ```
   ./dbgen -f -s 0.01
   ```
3. Convert them to Avro. `TblToAvro` writes one `<table>.avro` file for each table into the same folder:
   ```
   java -cp <test classpath> org.apache.pinot.integration.tests.tpch.TblToAvro <folder with the .tbl files>
   ```
4. Copy each `<table>.avro` file to `pinot-tools/src/main/resources/examples/batch/tpch/<table>/rawdata/`.
5. If the scale factor changes, update the row counts in `TPCHQueryIntegrationTest#NUM_ROWS`.
