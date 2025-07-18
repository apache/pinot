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

## isFalse

### Description

Returns true if the input is false (0), otherwise false. Null inputs return false.
### Summary

| Scenario | Semantic |
|----------|----------|
| Ingestion time transformer | ❌ Unsupported |
| MSE intermediate stage (with null handling) | ❌ "QueryValidationError: From line 15, column 3 to line 15, column 19: No match found for function signature isFalse(<NUMERIC>). From line 15, column 3 to line 15, column 19: No match found for function signature isFalse(<NUMERIC>). No match found for function signature isFalse(<NUMERIC>)" |
| MSE intermediate stage (without null handling) | ❌ "QueryValidationError: From line 14, column 3 to line 14, column 19: No match found for function signature isFalse(<NUMERIC>). From line 14, column 3 to line 14, column 19: No match found for function signature isFalse(<NUMERIC>). No match found for function signature isFalse(<NUMERIC>)" |
| SSE predicate (with null handling) | EQUAL |
| SSE predicate (without null handling) | EQUAL |
| SSE projection (with null handling) | EQUAL |
| SSE projection (without null handling) | EQUAL with 1 errors. |
### Signatures

#### isFalse(input: int) -> boolean

| Parameter | Type | Description |
|-----------|------|-------------|
| input | int | Input value to check if it is false (0) |
### Scenarios

#### Ingestion time transformer


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (input: int) -> boolean | - | - | - | ❌ Unsupported |


#### MSE intermediate stage (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (input: int) -> boolean | - | - | - | ❌ "QueryValidationError: From line 15, column 3 to line 15, column 19: No match found for function signature isFalse(<NUMERIC>). From line 15, column 3 to line 15, column 19: No match found for function signature isFalse(<NUMERIC>). No match found for function signature isFalse(<NUMERIC>)" |


#### MSE intermediate stage (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (input: int) -> boolean | - | - | - | ❌ "QueryValidationError: From line 14, column 3 to line 14, column 19: No match found for function signature isFalse(<NUMERIC>). From line 14, column 3 to line 14, column 19: No match found for function signature isFalse(<NUMERIC>). No match found for function signature isFalse(<NUMERIC>)" |


#### SSE predicate (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (input: int) -> boolean |isFalse(0) |true |true |EQUAL |
| (input: int) -> boolean |isFalse(NULL) |true |true |EQUAL |
| (input: int) -> boolean |isFalse(-1) |true |true |EQUAL |
| (input: int) -> boolean |isFalse(1) |true |true |EQUAL |


#### SSE predicate (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (input: int) -> boolean |isFalse(0) |true |true |EQUAL |
| (input: int) -> boolean |isFalse(NULL) |true |true |EQUAL |
| (input: int) -> boolean |isFalse(-1) |true |true |EQUAL |
| (input: int) -> boolean |isFalse(1) |true |true |EQUAL |


#### SSE projection (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (input: int) -> boolean |isFalse(0) |true |true |EQUAL |
| (input: int) -> boolean |isFalse(NULL) |false |false |EQUAL |
| (input: int) -> boolean |isFalse(-1) |false |false |EQUAL |
| (input: int) -> boolean |isFalse(1) |false |false |EQUAL |


#### SSE projection (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (input: int) -> boolean |isFalse(0) |true |true |EQUAL |
| (input: int) -> boolean |isFalse(NULL) |false |true |❌ Unexpected value |
| (input: int) -> boolean |isFalse(-1) |false |false |EQUAL |
| (input: int) -> boolean |isFalse(1) |false |false |EQUAL |


