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

## mult

Other names: times

### Description

This function multiplies two numeric values together.
### Summary

|Call | Result (with null handling) | Result (without null handling)
|-----|-----------------------------|------------------------------|
| (2.0 * NULL) | NULL | 0.0 |
| (2.0 * 3.0) | 6.0 | 6.0 |

This UDF has different semantics in different scenarios:

| Scenario | Semantic |
|----------|----------|
| Ingestion time transformer | NUMBER_AS_DOUBLE |
| MSE intermediate stage (with null handling) | EQUAL with 1 errors. |
| MSE intermediate stage (without null handling) | BIG_DECIMAL_AS_DOUBLE with 1 errors. |
| SSE predicate (with null handling) | EQUAL |
| SSE predicate (without null handling) | EQUAL |
| SSE projection (with null handling) | NUMBER_AS_DOUBLE with 1 errors. |
| SSE projection (without null handling) | NUMBER_AS_DOUBLE with 1 errors. |
### Signatures

#### mult(arg0: big_decimal, arg1: big_decimal) -> big_decimal

#### mult(arg0: double, arg1: double) -> double

#### mult(arg0: float, arg1: float) -> float

#### mult(arg0: int, arg1: int) -> int

#### mult(arg0: long, arg1: long) -> long

### Scenarios

<details>

<summary>Click to open</summary>

#### Ingestion time transformer


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(2.0 * NULL) |NULL |NULL |EQUAL |
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(2.0 * 3.0) |6.0 (BigDecimal) |6.0 (Double) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: double, arg1: double) -> double |(2.0 * 3.0) |6.0 |6.0 |EQUAL |
| (arg0: double, arg1: double) -> double |(2.0 * NULL) |NULL |NULL |EQUAL |
| (arg0: float, arg1: float) -> float |(2.0 * NULL) |NULL |NULL |EQUAL |
| (arg0: float, arg1: float) -> float |(2.0 * 3.0) |6.0 (Float) |6.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int, arg1: int) -> int |(2 * 3) |6 (Integer) |6.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int, arg1: int) -> int |(2 * NULL) |NULL |NULL |EQUAL |
| (arg0: long, arg1: long) -> long |(2 * NULL) |NULL |NULL |EQUAL |
| (arg0: long, arg1: long) -> long |(2 * 3) |6 (Long) |6.0 (Double) |NUMBER_AS_DOUBLE |

#### MSE intermediate stage (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(2.0 * NULL) |NULL |NULL |EQUAL |
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(2.0 * 3.0) |6.0 (BigDecimal) |0.0 (Double) |❌ Unexpected value |
| (arg0: double, arg1: double) -> double |(2.0 * 3.0) |6.0 |6.0 |EQUAL |
| (arg0: double, arg1: double) -> double |(2.0 * NULL) |NULL |NULL |EQUAL |
| (arg0: float, arg1: float) -> float |(2.0 * NULL) |NULL |NULL |EQUAL |
| (arg0: float, arg1: float) -> float |(2.0 * 3.0) |6.0 |6.0 |EQUAL |
| (arg0: int, arg1: int) -> int |(2 * 3) |6 |6 |EQUAL |
| (arg0: int, arg1: int) -> int |(2 * NULL) |NULL |NULL |EQUAL |
| (arg0: long, arg1: long) -> long |(2 * NULL) |NULL |NULL |EQUAL |
| (arg0: long, arg1: long) -> long |(2 * 3) |6 |6 |EQUAL |

#### MSE intermediate stage (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(2.0 * NULL) |0.0 (BigDecimal) |0.0 (Double) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(2.0 * 3.0) |6.0 (BigDecimal) |0.0 (Double) |❌ Unexpected value |
| (arg0: double, arg1: double) -> double |(2.0 * 3.0) |6.0 |6.0 |EQUAL |
| (arg0: double, arg1: double) -> double |(2.0 * NULL) |0.0 |0.0 |EQUAL |
| (arg0: float, arg1: float) -> float |(2.0 * NULL) |0.0 |0.0 |EQUAL |
| (arg0: float, arg1: float) -> float |(2.0 * 3.0) |6.0 |6.0 |EQUAL |
| (arg0: int, arg1: int) -> int |(2 * 3) |6 |6 |EQUAL |
| (arg0: int, arg1: int) -> int |(2 * NULL) |0 |0 |EQUAL |
| (arg0: long, arg1: long) -> long |(2 * NULL) |0 |0 |EQUAL |
| (arg0: long, arg1: long) -> long |(2 * 3) |6 |6 |EQUAL |

#### SSE predicate (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(2.0 * NULL) |true |true |EQUAL |
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(2.0 * 3.0) |true |true |EQUAL |
| (arg0: double, arg1: double) -> double |(2.0 * 3.0) |true |true |EQUAL |
| (arg0: double, arg1: double) -> double |(2.0 * NULL) |true |true |EQUAL |
| (arg0: float, arg1: float) -> float |(2.0 * NULL) |true |true |EQUAL |
| (arg0: float, arg1: float) -> float |(2.0 * 3.0) |true |true |EQUAL |
| (arg0: int, arg1: int) -> int |(2 * 3) |true |true |EQUAL |
| (arg0: int, arg1: int) -> int |(2 * NULL) |true |true |EQUAL |
| (arg0: long, arg1: long) -> long |(2 * NULL) |true |true |EQUAL |
| (arg0: long, arg1: long) -> long |(2 * 3) |true |true |EQUAL |

#### SSE predicate (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(2.0 * NULL) |true |true |EQUAL |
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(2.0 * 3.0) |true |true |EQUAL |
| (arg0: double, arg1: double) -> double |(2.0 * 3.0) |true |true |EQUAL |
| (arg0: double, arg1: double) -> double |(2.0 * NULL) |true |true |EQUAL |
| (arg0: float, arg1: float) -> float |(2.0 * NULL) |true |true |EQUAL |
| (arg0: float, arg1: float) -> float |(2.0 * 3.0) |true |true |EQUAL |
| (arg0: int, arg1: int) -> int |(2 * 3) |true |true |EQUAL |
| (arg0: int, arg1: int) -> int |(2 * NULL) |true |true |EQUAL |
| (arg0: long, arg1: long) -> long |(2 * NULL) |true |true |EQUAL |
| (arg0: long, arg1: long) -> long |(2 * 3) |true |true |EQUAL |

#### SSE projection (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(2.0 * NULL) |NULL |NULL |EQUAL |
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(2.0 * 3.0) |6.0 (BigDecimal) |0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000040 (String) |❌ Unexpected value |
| (arg0: double, arg1: double) -> double |(2.0 * 3.0) |6.0 |6.0 |EQUAL |
| (arg0: double, arg1: double) -> double |(2.0 * NULL) |NULL |NULL |EQUAL |
| (arg0: float, arg1: float) -> float |(2.0 * NULL) |NULL |NULL |EQUAL |
| (arg0: float, arg1: float) -> float |(2.0 * 3.0) |6.0 (Float) |6.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int, arg1: int) -> int |(2 * 3) |6 (Integer) |6.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int, arg1: int) -> int |(2 * NULL) |NULL |NULL |EQUAL |
| (arg0: long, arg1: long) -> long |(2 * NULL) |NULL |NULL |EQUAL |
| (arg0: long, arg1: long) -> long |(2 * 3) |6 (Long) |6.0 (Double) |NUMBER_AS_DOUBLE |

#### SSE projection (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(2.0 * NULL) |0.0 (BigDecimal) |0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000 (String) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: big_decimal, arg1: big_decimal) -> big_decimal |(2.0 * 3.0) |6.0 (BigDecimal) |0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000040 (String) |❌ Unexpected value |
| (arg0: double, arg1: double) -> double |(2.0 * 3.0) |6.0 |6.0 |EQUAL |
| (arg0: double, arg1: double) -> double |(2.0 * NULL) |0.0 |0.0 |EQUAL |
| (arg0: float, arg1: float) -> float |(2.0 * NULL) |0.0 (Float) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: float, arg1: float) -> float |(2.0 * 3.0) |6.0 (Float) |6.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int, arg1: int) -> int |(2 * 3) |6 (Integer) |6.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int, arg1: int) -> int |(2 * NULL) |0 (Integer) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long, arg1: long) -> long |(2 * NULL) |0 (Long) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long, arg1: long) -> long |(2 * 3) |6 (Long) |6.0 (Double) |NUMBER_AS_DOUBLE |


</details>

