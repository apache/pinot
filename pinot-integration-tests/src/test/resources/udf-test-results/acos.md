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

## acos

### Description

Returns the arc cosine of a numeric input (in radians).
### Summary

The UDF acos is supported in all scenarios with at least EQUAL semantic.

### Signatures

#### acos(d: double) -> double

The arc cosine of the input value, in radians.

| Parameter | Type | Description |
|-----------|------|-------------|
| d | double | The double value for which to compute the arc cosine. |
### Scenarios

#### Ingestion time transformer


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (d: double) -> double |acos(NULL) |NULL |NULL |EQUAL |
| (d: double) -> double |acos(0.0) |1.5707963267948966 |1.5707963267948966 |EQUAL |
| (d: double) -> double |acos(1.0) |0.0 |0.0 |EQUAL |
| (d: double) -> double |acos(-10.0) |NaN |NaN |EQUAL |
| (d: double) -> double |acos(10.0) |NaN |NaN |EQUAL |
| (d: double) -> double |acos(-1.0) |3.141592653589793 |3.141592653589793 |EQUAL |


#### MSE intermediate stage (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (d: double) -> double |acos(-10.0) |NaN |NaN |EQUAL |
| (d: double) -> double |acos(10.0) |NaN |NaN |EQUAL |
| (d: double) -> double |acos(NULL) |NULL |NULL |EQUAL |
| (d: double) -> double |acos(0.0) |1.5707963267948966 |1.5707963267948966 |EQUAL |
| (d: double) -> double |acos(1.0) |0.0 |0.0 |EQUAL |
| (d: double) -> double |acos(-1.0) |3.141592653589793 |3.141592653589793 |EQUAL |


#### MSE intermediate stage (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (d: double) -> double |acos(-10.0) |NaN |NaN |EQUAL |
| (d: double) -> double |acos(10.0) |NaN |NaN |EQUAL |
| (d: double) -> double |acos(NULL) |1.5707963267948966 |1.5707963267948966 |EQUAL |
| (d: double) -> double |acos(0.0) |1.5707963267948966 |1.5707963267948966 |EQUAL |
| (d: double) -> double |acos(1.0) |0.0 |0.0 |EQUAL |
| (d: double) -> double |acos(-1.0) |3.141592653589793 |3.141592653589793 |EQUAL |


#### SSE predicate (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (d: double) -> double |acos(NULL) |true |true |EQUAL |
| (d: double) -> double |acos(0.0) |true |true |EQUAL |
| (d: double) -> double |acos(1.0) |true |true |EQUAL |
| (d: double) -> double |acos(-10.0) |true |true |EQUAL |
| (d: double) -> double |acos(10.0) |true |true |EQUAL |
| (d: double) -> double |acos(-1.0) |true |true |EQUAL |


#### SSE predicate (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (d: double) -> double |acos(NULL) |true |true |EQUAL |
| (d: double) -> double |acos(0.0) |true |true |EQUAL |
| (d: double) -> double |acos(1.0) |true |true |EQUAL |
| (d: double) -> double |acos(-10.0) |true |true |EQUAL |
| (d: double) -> double |acos(10.0) |true |true |EQUAL |
| (d: double) -> double |acos(-1.0) |true |true |EQUAL |


#### SSE projection (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (d: double) -> double |acos(-10.0) |NaN |NaN |EQUAL |
| (d: double) -> double |acos(10.0) |NaN |NaN |EQUAL |
| (d: double) -> double |acos(NULL) |NULL |NULL |EQUAL |
| (d: double) -> double |acos(0.0) |1.5707963267948966 |1.5707963267948966 |EQUAL |
| (d: double) -> double |acos(1.0) |0.0 |0.0 |EQUAL |
| (d: double) -> double |acos(-1.0) |3.141592653589793 |3.141592653589793 |EQUAL |


#### SSE projection (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (d: double) -> double |acos(-10.0) |NaN |NaN |EQUAL |
| (d: double) -> double |acos(10.0) |NaN |NaN |EQUAL |
| (d: double) -> double |acos(NULL) |1.5707963267948966 |1.5707963267948966 |EQUAL |
| (d: double) -> double |acos(0.0) |1.5707963267948966 |1.5707963267948966 |EQUAL |
| (d: double) -> double |acos(1.0) |0.0 |0.0 |EQUAL |
| (d: double) -> double |acos(-1.0) |3.141592653589793 |3.141592653589793 |EQUAL |


