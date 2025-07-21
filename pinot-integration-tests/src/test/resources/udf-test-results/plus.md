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

## plus

Other names: add

### Description

This function adds two numeric values together. In order to concatenate two strings, use the `concat` function instead.
### Summary

|Call | Result (with null handling) | Result (without null handling)
|-----|-----------------------------|------------------------------|
| (1.0 + NULL) | NULL | 1.0 |
| (1.0 + 2.0) | 3.0 | 3.0 |

This UDF has different semantics in different scenarios:

| Scenario | Semantic |
|----------|----------|
| Ingestion time transformer | NUMBER_AS_DOUBLE |
| MSE intermediate stage (with null handling) | EQUAL with 1 errors. |
| MSE intermediate stage (without null handling) | EQUAL with 2 errors. |
| SSE predicate (with null handling) | EQUAL |
| SSE predicate (without null handling) | EQUAL |
| SSE projection (with null handling) | NUMBER_AS_DOUBLE with 1 errors. |
| SSE projection (without null handling) | NUMBER_AS_DOUBLE with 2 errors. |
### Signatures

#### plus(arg0: big_decimal, arg1: big_decimal) -> big_decimal

#### plus(arg0: double, arg1: double) -> double

#### plus(arg0: float, arg1: float) -> float

#### plus(arg0: int, arg1: int) -> int

#### plus(arg0: long, arg1: long) -> long

### Scenarios

<details>

<summary>Click to open</summary>

#### Ingestion time transformer


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(1.0 + NULL) |NULL |NULL |EQUAL |
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(1.0 + 2.0) |3.0 (BigDecimal) |3.0 (Double) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: double, arg1: double) -> double |(1.0 + NULL) |NULL |NULL |EQUAL |
| (arg0: double, arg1: double) -> double |(1.0 + 2.0) |3.0 |3.0 |EQUAL |
| (arg0: float, arg1: float) -> float |(1.0 + 2.0) |3.0 (Float) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: float, arg1: float) -> float |(1.0 + NULL) |NULL |NULL |EQUAL |
| (arg0: int, arg1: int) -> int |(1 + NULL) |NULL |NULL |EQUAL |
| (arg0: int, arg1: int) -> int |(1 + 2) |3 (Integer) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long, arg1: long) -> long |(1 + 2) |3 (Long) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long, arg1: long) -> long |(1 + NULL) |NULL |NULL |EQUAL |

#### MSE intermediate stage (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(1.0 + 2.0) |3.0 (BigDecimal) |0.0 (Double) |❌ Unexpected value |
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(1.0 + NULL) |NULL |NULL |EQUAL |
| (arg0: double, arg1: double) -> double |(1.0 + NULL) |NULL |NULL |EQUAL |
| (arg0: double, arg1: double) -> double |(1.0 + 2.0) |3.0 |3.0 |EQUAL |
| (arg0: float, arg1: float) -> float |(1.0 + 2.0) |3.0 |3.0 |EQUAL |
| (arg0: float, arg1: float) -> float |(1.0 + NULL) |NULL |NULL |EQUAL |
| (arg0: int, arg1: int) -> int |(1 + NULL) |NULL |NULL |EQUAL |
| (arg0: int, arg1: int) -> int |(1 + 2) |3 |3 |EQUAL |
| (arg0: long, arg1: long) -> long |(1 + NULL) |NULL |NULL |EQUAL |
| (arg0: long, arg1: long) -> long |(1 + 2) |3 |3 |EQUAL |

#### MSE intermediate stage (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(1.0 + 2.0) |3.0 (BigDecimal) |0.0 (Double) |❌ Unexpected value |
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(1.0 + NULL) |1.0 (BigDecimal) |0.0 (Double) |❌ Unexpected value |
| (arg0: double, arg1: double) -> double |(1.0 + NULL) |1.0 |1.0 |EQUAL |
| (arg0: double, arg1: double) -> double |(1.0 + 2.0) |3.0 |3.0 |EQUAL |
| (arg0: float, arg1: float) -> float |(1.0 + 2.0) |3.0 |3.0 |EQUAL |
| (arg0: float, arg1: float) -> float |(1.0 + NULL) |1.0 |1.0 |EQUAL |
| (arg0: int, arg1: int) -> int |(1 + NULL) |1 |1 |EQUAL |
| (arg0: int, arg1: int) -> int |(1 + 2) |3 |3 |EQUAL |
| (arg0: long, arg1: long) -> long |(1 + NULL) |1 |1 |EQUAL |
| (arg0: long, arg1: long) -> long |(1 + 2) |3 |3 |EQUAL |

#### SSE predicate (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(1.0 + NULL) |true |true |EQUAL |
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(1.0 + 2.0) |true |true |EQUAL |
| (arg0: double, arg1: double) -> double |(1.0 + NULL) |true |true |EQUAL |
| (arg0: double, arg1: double) -> double |(1.0 + 2.0) |true |true |EQUAL |
| (arg0: float, arg1: float) -> float |(1.0 + 2.0) |true |true |EQUAL |
| (arg0: float, arg1: float) -> float |(1.0 + NULL) |true |true |EQUAL |
| (arg0: int, arg1: int) -> int |(1 + NULL) |true |true |EQUAL |
| (arg0: int, arg1: int) -> int |(1 + 2) |true |true |EQUAL |
| (arg0: long, arg1: long) -> long |(1 + 2) |true |true |EQUAL |
| (arg0: long, arg1: long) -> long |(1 + NULL) |true |true |EQUAL |

#### SSE predicate (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(1.0 + NULL) |true |true |EQUAL |
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(1.0 + 2.0) |true |true |EQUAL |
| (arg0: double, arg1: double) -> double |(1.0 + NULL) |true |true |EQUAL |
| (arg0: double, arg1: double) -> double |(1.0 + 2.0) |true |true |EQUAL |
| (arg0: float, arg1: float) -> float |(1.0 + 2.0) |true |true |EQUAL |
| (arg0: float, arg1: float) -> float |(1.0 + NULL) |true |true |EQUAL |
| (arg0: int, arg1: int) -> int |(1 + NULL) |true |true |EQUAL |
| (arg0: int, arg1: int) -> int |(1 + 2) |true |true |EQUAL |
| (arg0: long, arg1: long) -> long |(1 + 2) |true |true |EQUAL |
| (arg0: long, arg1: long) -> long |(1 + NULL) |true |true |EQUAL |

#### SSE projection (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(1.0 + 2.0) |3.0 (BigDecimal) |0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020000000002 (String) |❌ Unexpected value |
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(1.0 + NULL) |NULL |NULL |EQUAL |
| (arg0: double, arg1: double) -> double |(1.0 + NULL) |NULL |NULL |EQUAL |
| (arg0: double, arg1: double) -> double |(1.0 + 2.0) |3.0 |3.0 |EQUAL |
| (arg0: float, arg1: float) -> float |(1.0 + 2.0) |3.0 (Float) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: float, arg1: float) -> float |(1.0 + NULL) |NULL |NULL |EQUAL |
| (arg0: int, arg1: int) -> int |(1 + NULL) |NULL |NULL |EQUAL |
| (arg0: int, arg1: int) -> int |(1 + 2) |3 (Integer) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long, arg1: long) -> long |(1 + NULL) |NULL |NULL |EQUAL |
| (arg0: long, arg1: long) -> long |(1 + 2) |3 (Long) |3.0 (Double) |NUMBER_AS_DOUBLE |

#### SSE projection (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(1.0 + 2.0) |3.0 (BigDecimal) |0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020000000002 (String) |❌ Unexpected value |
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(1.0 + NULL) |1.0 (BigDecimal) |0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002 (String) |❌ Unexpected value |
| (arg0: double, arg1: double) -> double |(1.0 + NULL) |1.0 |1.0 |EQUAL |
| (arg0: double, arg1: double) -> double |(1.0 + 2.0) |3.0 |3.0 |EQUAL |
| (arg0: float, arg1: float) -> float |(1.0 + 2.0) |3.0 (Float) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: float, arg1: float) -> float |(1.0 + NULL) |1.0 (Float) |1.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int, arg1: int) -> int |(1 + NULL) |1 (Integer) |1.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int, arg1: int) -> int |(1 + 2) |3 (Integer) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long, arg1: long) -> long |(1 + NULL) |1 (Long) |1.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long, arg1: long) -> long |(1 + 2) |3 (Long) |3.0 (Double) |NUMBER_AS_DOUBLE |


</details>

