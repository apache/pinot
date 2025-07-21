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

## not

### Description

Logical NOT function for a boolean value. Returns true if the argument is false, false if true, and null if null.
### Summary

|Call | Result (with null handling) | Result (without null handling)
|-----|-----------------------------|------------------------------|
| not(NULL) | NULL | true |
| not(false) | true | true |
| not(true) | false | false |

The UDF not is supported in all scenarios

### Signatures

#### not(value: boolean) -> boolean

Returns the logical negation of the input value.

| Parameter | Type | Description |
|-----------|------|-------------|
| value | boolean | A boolean value to negate. |
### Scenarios

<details>

<summary>Click to open</summary>

#### Ingestion time transformer


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (value: boolean) -> boolean |not(NULL) |NULL |NULL |EQUAL |
| (value: boolean) -> boolean |not(false) |true |true |EQUAL |
| (value: boolean) -> boolean |not(true) |false |false |EQUAL |

#### MSE intermediate stage (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (value: boolean) -> boolean |not(NULL) |NULL |NULL |EQUAL |
| (value: boolean) -> boolean |not(false) |true |true |EQUAL |
| (value: boolean) -> boolean |not(true) |false |false |EQUAL |

#### MSE intermediate stage (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (value: boolean) -> boolean |not(NULL) |true |true |EQUAL |
| (value: boolean) -> boolean |not(false) |true |true |EQUAL |
| (value: boolean) -> boolean |not(true) |false |false |EQUAL |

#### SSE predicate (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (value: boolean) -> boolean |not(NULL) |true |true |EQUAL |
| (value: boolean) -> boolean |not(false) |true |true |EQUAL |
| (value: boolean) -> boolean |not(true) |true |true |EQUAL |

#### SSE predicate (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (value: boolean) -> boolean |not(NULL) |true |true |EQUAL |
| (value: boolean) -> boolean |not(false) |true |true |EQUAL |
| (value: boolean) -> boolean |not(true) |true |true |EQUAL |

#### SSE projection (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (value: boolean) -> boolean |not(NULL) |NULL |NULL |EQUAL |
| (value: boolean) -> boolean |not(false) |true |true |EQUAL |
| (value: boolean) -> boolean |not(true) |false |false |EQUAL |

#### SSE projection (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (value: boolean) -> boolean |not(NULL) |true |true |EQUAL |
| (value: boolean) -> boolean |not(false) |true |true |EQUAL |
| (value: boolean) -> boolean |not(true) |false |false |EQUAL |


</details>

