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
# How to run advanced TPCHQueryIntegrationTest
## enrich data
1. Follow https://dev.mysql.com/doc/heatwave/en/mys-hw-tpch-sample-data.html to generate tpch data.
The current tpch dataset in the `resource` folder was generated using scale factor 0.0001, which is too small for the advanced query test.
```
./dbgen -vf -s 0.0001
```
If your encounter `fatal error: 'malloc.h' file not found` while building `dbgen` binary, please replace `malloc.h` with
`stdlib.h` in files having this problem.
2. Run TblToAvro to convert the data to avro format. Remember to provide the absolute folder path of those `tbl` files.
3. Copy those replace the current dataset in the `resource` folder.
```
export TPCH_AVRO_FILE_SOURCE_FOLDER={the absolute folder path of tbl files}
export PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER={the absolute folder path of pinot tpch integration test source folder
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/customer.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/lineitem.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/nation.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/orders.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/part.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/partsupp.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/region.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
cp $TPCH_AVRO_FILE_SOURCE_FOLDER/supplier.avro $PINOT_TPCH_INTEGRATION_TEST_SOURCE_FOLDER
```
