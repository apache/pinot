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

`delta_encoding_required_column.parquet` is generated with parquet-mr version 1.12.1.
The expected file contents are in `delta_encoding_required_column_expect.csv`.

All columns are required. INT32 columns are DELTA_BINARY_PACKED-encoded and STRING
columns are DELTA_BYTE_ARRAY-encoded. Each column has 100 rows.

Here is the file structure:
```file:                   file:/Users/pincheng/arrow/cpp/submodules/parquet-testing/data/delta_encoding_required_column.parquet
creator:                parquet-mr version 1.12.1 (build 2a5c06c58fa987f85aa22170be14d927d5ff6e7d)
extra:                  org.apache.spark.version = 3.2.0
extra:                  org.apache.spark.sql.parquet.row.metadata = {"type":"struct","fields":[{"name":"c_customer_sk:","type":"integer","nullable":false,"metadata":{}},{"name":"c_current_cdemo_sk:","type":"integer","nullable":false,"metadata":{}},{"name":"c_current_hdemo_sk:","type":"integer","nullable":false,"metadata":{}},{"name":"c_current_addr_sk:","type":"integer","nullable":false,"metadata":{}},{"name":"c_first_shipto_date_sk:","type":"integer","nullable":false,"metadata":{}},{"name":"c_first_sales_date_sk:","type":"integer","nullable":false,"metadata":{}},{"name":"c_birth_day:","type":"integer","nullable":false,"metadata":{}},{"name":"c_birth_month:","type":"integer","nullable":false,"metadata":{}},{"name":"c_birth_year:","type":"integer","nullable":false,"metadata":{}},{"name":"c_customer_id:","type":"string","nullable":false,"metadata":{}},{"name":"c_salutation:","type":"string","nullable":false,"metadata":{}},{"name":"c_first_name:","type":"string","nullable":false,"metadata":{}},{"name":"c_last_name:","type":"string","nullable":false,"metadata":{}},{"name":"c_preferred_cust_flag:","type":"string","nullable":false,"metadata":{}},{"name":"c_birth_country:","type":"string","nullable":false,"metadata":{}},{"name":"c_email_address:","type":"string","nullable":false,"metadata":{}},{"name":"c_last_review_date:","type":"string","nullable":false,"metadata":{}}]}

file schema:            spark_schema
--------------------------------------------------------------------------------
c_customer_sk:          : REQUIRED INT32 R:0 D:0
c_current_cdemo_sk:     : REQUIRED INT32 R:0 D:0
c_current_hdemo_sk:     : REQUIRED INT32 R:0 D:0
c_current_addr_sk:      : REQUIRED INT32 R:0 D:0
c_first_shipto_date_sk: : REQUIRED INT32 R:0 D:0
c_first_sales_date_sk:  : REQUIRED INT32 R:0 D:0
c_birth_day:            : REQUIRED INT32 R:0 D:0
c_birth_month:          : REQUIRED INT32 R:0 D:0
c_birth_year:           : REQUIRED INT32 R:0 D:0
c_customer_id:          : REQUIRED BINARY L:STRING R:0 D:0
c_salutation:           : REQUIRED BINARY L:STRING R:0 D:0
c_first_name:           : REQUIRED BINARY L:STRING R:0 D:0
c_last_name:            : REQUIRED BINARY L:STRING R:0 D:0
c_preferred_cust_flag:  : REQUIRED BINARY L:STRING R:0 D:0
c_birth_country:        : REQUIRED BINARY L:STRING R:0 D:0
c_email_address:        : REQUIRED BINARY L:STRING R:0 D:0
c_last_review_date:     : REQUIRED BINARY L:STRING R:0 D:0

row group 1:            RC:100 TS:9229 OFFSET:4
--------------------------------------------------------------------------------
c_customer_sk:          :  INT32 UNCOMPRESSED DO:0 FPO:4 SZ:50/50/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 1, max: 105, num_nulls: 0]
c_current_cdemo_sk:     :  INT32 UNCOMPRESSED DO:0 FPO:54 SZ:388/388/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 8817, max: 1895444, num_nulls: 0]
c_current_hdemo_sk:     :  INT32 UNCOMPRESSED DO:0 FPO:442 SZ:261/261/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 37, max: 7135, num_nulls: 0]
c_current_addr_sk:      :  INT32 UNCOMPRESSED DO:0 FPO:703 SZ:307/307/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 464, max: 49388, num_nulls: 0]
c_first_shipto_date_sk: :  INT32 UNCOMPRESSED DO:0 FPO:1010 SZ:247/247/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 2449130, max: 2452641, num_nulls: 0]
c_first_sales_date_sk:  :  INT32 UNCOMPRESSED DO:0 FPO:1257 SZ:247/247/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 2449100, max: 2452611, num_nulls: 0]
c_birth_day:            :  INT32 UNCOMPRESSED DO:0 FPO:1504 SZ:131/131/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 1, max: 30, num_nulls: 0]
c_birth_month:          :  INT32 UNCOMPRESSED DO:0 FPO:1635 SZ:115/115/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 1, max: 12, num_nulls: 0]
c_birth_year:           :  INT32 UNCOMPRESSED DO:0 FPO:1750 SZ:144/144/1.00 VC:100 ENC:DELTA_BINARY_PACKED ST:[min: 1925, max: 1991, num_nulls: 0]
c_customer_id:          :  BINARY UNCOMPRESSED DO:0 FPO:1894 SZ:933/933/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: AAAAAAAAABAAAAAA, max: AAAAAAAAPFAAAAAA, num_nulls: 0]
c_salutation:           :  BINARY UNCOMPRESSED DO:0 FPO:2827 SZ:378/378/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: Dr., max: Sir, num_nulls: 0]
c_first_name:           :  BINARY UNCOMPRESSED DO:0 FPO:3205 SZ:707/707/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: Albert, max: William, num_nulls: 0]
c_last_name:            :  BINARY UNCOMPRESSED DO:0 FPO:3912 SZ:751/751/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: Baker, max: Young, num_nulls: 0]
c_preferred_cust_flag:  :  BINARY UNCOMPRESSED DO:0 FPO:4663 SZ:154/154/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: N, max: Y, num_nulls: 0]
c_birth_country:        :  BINARY UNCOMPRESSED DO:0 FPO:4817 SZ:1154/1154/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: AFGHANISTAN, max: WALLIS AND FUTUNA, num_nulls: 0]
c_email_address:        :  BINARY UNCOMPRESSED DO:0 FPO:5971 SZ:2857/2857/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: Albert.Brunson@62.com, max: William.Warner@zegnrzurU.org, num_nulls: 0]
c_last_review_date:     :  BINARY UNCOMPRESSED DO:0 FPO:8828 SZ:405/405/1.00 VC:100 ENC:DELTA_BYTE_ARRAY ST:[min: 2452293, max: 2452644, num_nulls: 0]
```
