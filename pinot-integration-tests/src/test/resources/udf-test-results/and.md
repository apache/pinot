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

## and

### Description

Stub for and function. Not implemented.
### Summary

The UDF and is supported in all scenarios with at least EQUAL semantic.

### Signatures

#### and(left: boolean, right: boolean) -> boolean

Result of the AND operation, true if both operands are true, false otherwise

| Parameter | Type | Description |
|-----------|------|-------------|
| left | boolean | Left operand of the AND operation |
| right | boolean | Right operand of the AND operation |
### Scenarios

#### Ingestion time transformer


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (left: boolean, right: boolean) -> boolean |and(false, true) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, NULL) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(NULL, NULL) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(false, false) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(NULL, true) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, false) |false |false |EQUAL |


#### MSE intermediate stage (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (left: boolean, right: boolean) -> boolean |and(false, true) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, NULL) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(NULL, NULL) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(false, false) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(NULL, true) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, false) |false |false |EQUAL |


#### MSE intermediate stage (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (left: boolean, right: boolean) -> boolean |and(false, true) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, NULL) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(NULL, NULL) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(false, false) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(NULL, true) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, false) |false |false |EQUAL |


#### SSE predicate (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (left: boolean, right: boolean) -> boolean |and(false, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, NULL) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(NULL, NULL) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(false, false) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(NULL, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, false) |true |true |EQUAL |


#### SSE predicate (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (left: boolean, right: boolean) -> boolean |and(false, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, NULL) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(NULL, NULL) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(false, false) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(NULL, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, false) |true |true |EQUAL |


#### SSE projection (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (left: boolean, right: boolean) -> boolean |and(false, true) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, NULL) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(NULL, NULL) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(false, false) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(NULL, true) |NULL |NULL |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, false) |false |false |EQUAL |


#### SSE projection (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (left: boolean, right: boolean) -> boolean |and(false, true) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, NULL) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(NULL, NULL) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(false, false) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, true) |true |true |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(NULL, true) |false |false |EQUAL |
| (left: boolean, right: boolean) -> boolean |and(true, false) |false |false |EQUAL |


