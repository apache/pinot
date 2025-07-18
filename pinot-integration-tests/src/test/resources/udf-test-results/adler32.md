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

## adler32

### Description

Stub for adler32 function. Not implemented.
### Summary

The UDF adler32 is supported in all scenarios with at least EQUAL semantic.

### Signatures

#### adler32(input: bytes) -> int

The Adler-32 checksum of the input byte array

| Parameter | Type | Description |
|-----------|------|-------------|
| input | bytes | Input byte array to compute the Adler-32 checksum |
### Scenarios

#### Ingestion time transformer


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (input: bytes) -> int |adler32(hexToBytes('01')) |131074 |131074 |EQUAL |
| (input: bytes) -> int |adler32(hexToBytes('01020304')) |1572875 |1572875 |EQUAL |
| (input: bytes) -> int |adler32(NULL) |NULL |NULL |EQUAL |
| (input: bytes) -> int |adler32(hexToBytes('')) |1 |1 |EQUAL |


#### MSE intermediate stage (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (input: bytes) -> int |adler32(hexToBytes('01')) |131074 |131074 |EQUAL |
| (input: bytes) -> int |adler32(hexToBytes('01020304')) |1572875 |1572875 |EQUAL |
| (input: bytes) -> int |adler32(NULL) |NULL |NULL |EQUAL |
| (input: bytes) -> int |adler32(hexToBytes('')) |1 |1 |EQUAL |


#### MSE intermediate stage (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (input: bytes) -> int |adler32(hexToBytes('01')) |131074 |131074 |EQUAL |
| (input: bytes) -> int |adler32(hexToBytes('01020304')) |1572875 |1572875 |EQUAL |
| (input: bytes) -> int |adler32(NULL) |1 |1 |EQUAL |
| (input: bytes) -> int |adler32(hexToBytes('')) |1 |1 |EQUAL |


#### SSE predicate (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (input: bytes) -> int |adler32(hexToBytes('01')) |true |true |EQUAL |
| (input: bytes) -> int |adler32(hexToBytes('01020304')) |true |true |EQUAL |
| (input: bytes) -> int |adler32(NULL) |true |true |EQUAL |
| (input: bytes) -> int |adler32(hexToBytes('')) |true |true |EQUAL |


#### SSE predicate (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (input: bytes) -> int |adler32(hexToBytes('01')) |true |true |EQUAL |
| (input: bytes) -> int |adler32(hexToBytes('01020304')) |true |true |EQUAL |
| (input: bytes) -> int |adler32(NULL) |true |true |EQUAL |
| (input: bytes) -> int |adler32(hexToBytes('')) |true |true |EQUAL |


#### SSE projection (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (input: bytes) -> int |adler32(hexToBytes('01')) |131074 |131074 |EQUAL |
| (input: bytes) -> int |adler32(hexToBytes('01020304')) |1572875 |1572875 |EQUAL |
| (input: bytes) -> int |adler32(NULL) |NULL |NULL |EQUAL |
| (input: bytes) -> int |adler32(hexToBytes('')) |1 |1 |EQUAL |


#### SSE projection (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (input: bytes) -> int |adler32(hexToBytes('01')) |131074 |131074 |EQUAL |
| (input: bytes) -> int |adler32(hexToBytes('01020304')) |1572875 |1572875 |EQUAL |
| (input: bytes) -> int |adler32(NULL) |1 |1 |EQUAL |
| (input: bytes) -> int |adler32(hexToBytes('')) |1 |1 |EQUAL |


