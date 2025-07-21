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

## or

### Description

Logical OR function for two boolean values. Returns true if either argument is true, false if both are false, and null if both are null or one is null and the other is false.
### Summary

|Call | Result (with null handling) | Result (without null handling)
|-----|-----------------------------|------------------------------|
| or(NULL, true) | true | true |
| or(NULL, NULL) | NULL | false |
| or(false, true) | true | true |
| or(true, true) | true | true |
| or(false, false) | false | false |
| or(NULL, false) | NULL | false |
| or(false, NULL) | NULL | false |
| or(true, false) | true | true |
| or(true, NULL) | true | true |

The UDF or is supported in all scenarios

### Signatures

#### or(left: boolean, right: boolean) -> boolean

Result of the OR operation, true if either operand is true, false otherwise

| Parameter | Type | Description |
|-----------|------|-------------|
| left | boolean | Left operand of the OR operation |
| right | boolean | Right operand of the OR operation |
### Scenarios

<details>

<summary>Click to open</summary>

#### Ingestion time transformer


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (left: boolean, right: boolean) -> boolean |or(NULL, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(NULL, NULL) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, false) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(NULL, false) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, NULL) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, false) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, NULL) |true |true |EQUAL |

#### MSE intermediate stage (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (left: boolean, right: boolean) -> boolean |or(NULL, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(NULL, NULL) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, false) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(NULL, false) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, NULL) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, false) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, NULL) |true |true |EQUAL |

#### MSE intermediate stage (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (left: boolean, right: boolean) -> boolean |or(NULL, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(NULL, NULL) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, false) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(NULL, false) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, NULL) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, false) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, NULL) |true |true |EQUAL |

#### SSE predicate (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (left: boolean, right: boolean) -> boolean |or(NULL, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(NULL, NULL) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, false) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(NULL, false) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, NULL) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, false) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, NULL) |true |true |EQUAL |

#### SSE predicate (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (left: boolean, right: boolean) -> boolean |or(NULL, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(NULL, NULL) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, false) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(NULL, false) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, NULL) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, false) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, NULL) |true |true |EQUAL |

#### SSE projection (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (left: boolean, right: boolean) -> boolean |or(NULL, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(NULL, NULL) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, false) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(NULL, false) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, NULL) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, false) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, NULL) |true |true |EQUAL |

#### SSE projection (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (left: boolean, right: boolean) -> boolean |or(NULL, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(NULL, NULL) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, false) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(NULL, false) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(false, NULL) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, false) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |or(true, NULL) |true |true |EQUAL |


</details>

