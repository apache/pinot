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

## arrayElementAtInt

### Description

Returns the element at the specified index in an array of integers. The index is 1-based, meaning that the first element is at index 1. 
### Summary

|Call | Result (with null handling) | Result (without null handling)
|-----|-----------------------------|------------------------------|
| arrayElementAtInt([10, 20, 30], 2) | 20 | 20 |
| arrayElementAtInt([10, 20, 30], 0) | 0 | 0 |
| arrayElementAtInt([10, 20, 30], 4) | 0 | 0 |
| arrayElementAtInt([10, 20, 30], 1) | 10 | 10 |
| arrayElementAtInt([10, 20, 30], -1) | 0 | 0 |

The UDF arrayElementAtInt is supported in all scenarios

### Signatures

#### arrayElementAtInt(arr: ARRAY(int), idx: int) -> int

| Parameter | Type | Description |
|-----------|------|-------------|
| arr | int | Array of integers to retrieve the element from |
| idx | int | 1-based index of the element to retrieve. |
### Scenarios

<details>

<summary>Click to open</summary>

#### Ingestion time transformer


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 2) |20 |20 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 0) |0 |0 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 4) |0 |0 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 1) |10 |10 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], -1) |0 |0 |EQUAL |

#### MSE intermediate stage (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 0) |0 |0 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 4) |0 |0 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 2) |20 |20 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 1) |10 |10 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], -1) |0 |0 |EQUAL |

#### MSE intermediate stage (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 0) |0 |0 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 4) |0 |0 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 2) |20 |20 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 1) |10 |10 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], -1) |0 |0 |EQUAL |

#### SSE predicate (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 2) |true |true |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 0) |true |true |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 4) |true |true |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 1) |true |true |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], -1) |true |true |EQUAL |

#### SSE predicate (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 2) |true |true |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 0) |true |true |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 4) |true |true |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 1) |true |true |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], -1) |true |true |EQUAL |

#### SSE projection (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 0) |0 |0 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 4) |0 |0 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 2) |20 |20 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 1) |10 |10 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], -1) |0 |0 |EQUAL |

#### SSE projection (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 0) |0 |0 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 4) |0 |0 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 2) |20 |20 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], 1) |10 |10 |EQUAL |
| (arr: ARRAY(int), idx: int) -> int |arrayElementAtInt([10, 20, 30], -1) |0 |0 |EQUAL |


</details>

