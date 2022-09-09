<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

`delta_encoding_optional_column.parquet` is generated with parquet-mr version 1.10.0.
The expected file contents are in `delta_encoding_optional_column_expect.csv`.

All columns are optional. INT64 columns are DELTA_BINARY_PACKED-encoded and STRING
columns are DELTA_BYTE_ARRAY-encoded. Each column has 100 rows.

Here is the file structure:
```file:                   file:/Users/pincheng/arrow/cpp/submodules/parquet-testing/data/delta_encoding_optional_column.parquet
creator:                parquet-mr version 1.10.0 (build 031a6654009e3b82020012a18434c582bd74c73a)

file schema:            hive_schema
--------------------------------------------------------------------------------
c_customer_sk:          OPTIONAL INT64 R:0 D:1
c_current_cdemo_sk:     OPTIONAL INT64 R:0 D:1
c_current_hdemo_sk:     OPTIONAL INT64 R:0 D:1
c_current_addr_sk:      OPTIONAL INT64 R:0 D:1
c_first_shipto_date_sk: OPTIONAL INT64 R:0 D:1
c_first_sales_date_sk:  OPTIONAL INT64 R:0 D:1
c_birth_day:            OPTIONAL INT64 R:0 D:1
c_birth_month:          OPTIONAL INT64 R:0 D:1
c_birth_year:           OPTIONAL INT64 R:0 D:1
c_customer_id:          OPTIONAL BINARY L:STRING R:0 D:1
c_salutation:           OPTIONAL BINARY L:STRING R:0 D:1
c_first_name:           OPTIONAL BINARY L:STRING R:0 D:1
c_last_name:            OPTIONAL BINARY L:STRING R:0 D:1
c_preferred_cust_flag:  OPTIONAL BINARY L:STRING R:0 D:1
c_birth_country:        OPTIONAL BINARY L:STRING R:0 D:1
c_email_address:        OPTIONAL BINARY L:STRING R:0 D:1
c_last_review_date:     OPTIONAL BINARY L:STRING R:0 D:1

row group 1:            RC:100 TS:9485 OFFSET:4
--------------------------------------------------------------------------------
c_customer_sk:           INT64 UNCOMPRESSED DO:0 FPO:4 SZ:81/81/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 1, max: 100, num_nulls: 0]
c_current_cdemo_sk:      INT64 UNCOMPRESSED DO:0 FPO:85 SZ:358/358/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 8817, max: 1895444, num_nulls: 3]
c_current_hdemo_sk:      INT64 UNCOMPRESSED DO:0 FPO:443 SZ:311/311/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 37, max: 7135, num_nulls: 2]
c_current_addr_sk:       INT64 UNCOMPRESSED DO:0 FPO:754 SZ:353/353/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 571, max: 49388, num_nulls: 0]
c_first_shipto_date_sk:  INT64 UNCOMPRESSED DO:0 FPO:1107 SZ:297/297/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 2449130, max: 2452641, num_nulls: 1]
c_first_sales_date_sk:   INT64 UNCOMPRESSED DO:0 FPO:1404 SZ:297/297/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 2449010, max: 2452611, num_nulls: 1]
c_birth_day:             INT64 UNCOMPRESSED DO:0 FPO:1701 SZ:160/160/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 1, max: 30, num_nulls: 3]
c_birth_month:           INT64 UNCOMPRESSED DO:0 FPO:1861 SZ:146/146/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 1, max: 12, num_nulls: 3]
c_birth_year:            INT64 UNCOMPRESSED DO:0 FPO:2007 SZ:172/172/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 1925, max: 1991, num_nulls: 3]
c_customer_id:           BINARY UNCOMPRESSED DO:0 FPO:2179 SZ:976/976/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: AAAAAAAAABAAAAAA, max: AAAAAAAAPFAAAAAA, num_nulls: 0]
c_salutation:            BINARY UNCOMPRESSED DO:0 FPO:3155 SZ:376/376/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: Dr., max: Sir, num_nulls: 3]
c_first_name:            BINARY UNCOMPRESSED DO:0 FPO:3531 SZ:694/694/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: Albert, max: William, num_nulls: 3]
c_last_name:             BINARY UNCOMPRESSED DO:0 FPO:4225 SZ:777/777/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: Baker, max: Young, num_nulls: 1]
c_preferred_cust_flag:   BINARY UNCOMPRESSED DO:0 FPO:5002 SZ:156/156/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: N, max: Y, num_nulls: 4]
c_birth_country:         BINARY UNCOMPRESSED DO:0 FPO:5158 SZ:1111/1111/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: AFGHANISTAN, max: WALLIS AND FUTUNA, num_nulls: 4]
c_email_address:         BINARY UNCOMPRESSED DO:0 FPO:6269 SZ:2813/2813/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: Albert.Brunson@62.com, max: William.Warner@zegnrzurU.org, num_nulls: 3]
c_last_review_date:      BINARY UNCOMPRESSED DO:0 FPO:9082 SZ:407/407/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: 2452293, max: 2452644, num_nulls: 3]
```
