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

`delta_byte_array.parquet` is generated with parquet-mr version 1.10.0.
The expected file contents are in `delta_byte_array_expect.csv`.

All the column are DELTA_BYTE_ARRAY-encoded. Each column has 1000 rows.

Here is the file structure:
```file:                  file:/Users/pincheng/arrow/cpp/submodules/parquet-testing/data/delta_byte_array.parquet
creator:               parquet-mr version 1.10.0 (build 031a6654009e3b82020012a18434c582bd74c73a)

file schema:           hive_schema
--------------------------------------------------------------------------------
c_customer_id:         OPTIONAL BINARY L:STRING R:0 D:1
c_salutation:          OPTIONAL BINARY L:STRING R:0 D:1
c_first_name:          OPTIONAL BINARY L:STRING R:0 D:1
c_last_name:           OPTIONAL BINARY L:STRING R:0 D:1
c_preferred_cust_flag: OPTIONAL BINARY L:STRING R:0 D:1
c_birth_country:       OPTIONAL BINARY L:STRING R:0 D:1
c_login:               OPTIONAL BINARY L:STRING R:0 D:1
c_email_address:       OPTIONAL BINARY L:STRING R:0 D:1
c_last_review_date:    OPTIONAL BINARY L:STRING R:0 D:1

row group 1:           RC:1000 TS:67295 OFFSET:4
--------------------------------------------------------------------------------
c_customer_id:          BINARY UNCOMPRESSED DO:0 FPO:4 SZ:8248/8248/1.00 VC:1000 ENC:DELTA_BYTE_ARRAY ST:[min: AAAAAAAAAABAAAAA, max: AAAAAAAAPPCAAAAA, num_nulls: 0]
c_salutation:           BINARY UNCOMPRESSED DO:0 FPO:8252 SZ:3362/3362/1.00 VC:1000 ENC:DELTA_BYTE_ARRAY ST:[min: Dr., max: Sir, num_nulls: 30]
c_first_name:           BINARY UNCOMPRESSED DO:0 FPO:11614 SZ:6595/6595/1.00 VC:1000 ENC:DELTA_BYTE_ARRAY ST:[min: Aaron, max: Zachary, num_nulls: 32]
c_last_name:            BINARY UNCOMPRESSED DO:0 FPO:18209 SZ:6955/6955/1.00 VC:1000 ENC:DELTA_BYTE_ARRAY ST:[min: Adams, max: Zamora, num_nulls: 24]
c_preferred_cust_flag:  BINARY UNCOMPRESSED DO:0 FPO:25164 SZ:1220/1220/1.00 VC:1000 ENC:DELTA_BYTE_ARRAY ST:[min: N, max: Y, num_nulls: 29]
c_birth_country:        BINARY UNCOMPRESSED DO:0 FPO:26384 SZ:9599/9599/1.00 VC:1000 ENC:DELTA_BYTE_ARRAY ST:[min: AFGHANISTAN, max: ZIMBABWE, num_nulls: 31]
c_login:                BINARY UNCOMPRESSED DO:0 FPO:35983 SZ:42/42/1.00 VC:1000 ENC:DELTA_BYTE_ARRAY ST:[num_nulls: 1000, min/max not defined]
c_email_address:        BINARY UNCOMPRESSED DO:0 FPO:36025 SZ:27763/27763/1.00 VC:1000 ENC:DELTA_BYTE_ARRAY ST:[min: Aaron.Browder@iUpddkHI9z8.org, max: Zachary.Parsons@hHmnLrbKsfY.com, num_nulls: 31]
c_last_review_date:     BINARY UNCOMPRESSED DO:0 FPO:63788 SZ:3511/3511/1.00 VC:1000 ENC:DELTA_BYTE_ARRAY ST:[min: 2452283, max: 2452648, num_nulls: 25]
```
