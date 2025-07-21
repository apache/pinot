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

## year

### Description

Returns the year from the given epoch millis in UTC timezone.
### Summary

|Call | Result (with null handling) | Result (without null handling)
|-----|-----------------------------|------------------------------|
| year(1577836800000) | 2020 | 2020 |
| year(NULL) | NULL | 1970 |
| year(0) | 1970 | 1970 |

This UDF has different semantics in different scenarios:

| Scenario | Semantic |
|----------|----------|
| Ingestion time transformer | EQUAL |
| MSE intermediate stage (with null handling) | NUMBER_AS_DOUBLE |
| MSE intermediate stage (without null handling) | NUMBER_AS_DOUBLE |
| SSE predicate (with null handling) | EQUAL |
| SSE predicate (without null handling) | EQUAL |
| SSE projection (with null handling) | EQUAL |
| SSE projection (without null handling) | EQUAL |
### Signatures

#### year(millis: long) -> int

Returns the year as an integer

| Parameter | Type | Description |
|-----------|------|-------------|
| millis | long | A long value representing epoch millise.g., 1577836800000L for 2020-01-01T00:00:00Z |
### Scenarios

<details>

<summary>Click to open</summary>

#### Ingestion time transformer


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (millis: long) -> int |year(1577836800000) |2020 |2020 |EQUAL |
| (millis: long) -> int |year(NULL) |NULL |NULL |EQUAL |
| (millis: long) -> int |year(0) |1970 |1970 |EQUAL |

#### MSE intermediate stage (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (millis: long) -> int |year(1577836800000) |2020 (Integer) |2020 (Long) |NUMBER_AS_DOUBLE |
| (millis: long) -> int |year(NULL) |NULL |NULL |EQUAL |
| (millis: long) -> int |year(0) |1970 (Integer) |1970 (Long) |NUMBER_AS_DOUBLE |

#### MSE intermediate stage (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (millis: long) -> int |year(1577836800000) |2020 (Integer) |2020 (Long) |NUMBER_AS_DOUBLE |
| (millis: long) -> int |year(NULL) |1970 (Integer) |1970 (Long) |NUMBER_AS_DOUBLE |
| (millis: long) -> int |year(0) |1970 (Integer) |1970 (Long) |NUMBER_AS_DOUBLE |

#### SSE predicate (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (millis: long) -> int |year(1577836800000) |true |true |EQUAL |
| (millis: long) -> int |year(NULL) |true |true |EQUAL |
| (millis: long) -> int |year(0) |true |true |EQUAL |

#### SSE predicate (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (millis: long) -> int |year(1577836800000) |true |true |EQUAL |
| (millis: long) -> int |year(NULL) |true |true |EQUAL |
| (millis: long) -> int |year(0) |true |true |EQUAL |

#### SSE projection (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (millis: long) -> int |year(1577836800000) |2020 |2020 |EQUAL |
| (millis: long) -> int |year(NULL) |NULL |NULL |EQUAL |
| (millis: long) -> int |year(0) |1970 |1970 |EQUAL |

#### SSE projection (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (millis: long) -> int |year(1577836800000) |2020 |2020 |EQUAL |
| (millis: long) -> int |year(NULL) |1970 |1970 |EQUAL |
| (millis: long) -> int |year(0) |1970 |1970 |EQUAL |


</details>

