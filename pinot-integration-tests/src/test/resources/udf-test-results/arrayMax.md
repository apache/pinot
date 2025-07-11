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

## arrayMax

| Scenario | Semantic |
|----------|----------|
| Ingestion time transformer | ❌ Unsupported |
| MSE intermediate stage (with null handling) | ❌ Unsupported |
| MSE intermediate stage (without null handling) | ❌ Unsupported |
| SSE predicate (with null handling) | EQUAL |
| SSE predicate (without null handling) | EQUAL |
| SSE projection (with null handling) | EQUAL |
| SSE projection (without null handling) | EQUAL |
### Details

#### Ingestion time transformer


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| ([Lint) -> int | - | - | - | ❌ Unsupported |


#### MSE intermediate stage (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| ([Lint) -> int | - | - | - | ❌ Unsupported |


#### MSE intermediate stage (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| ([Lint) -> int | - | - | - | ❌ Unsupported |


#### SSE predicate (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| ([Lint) -> int |arrayMax([1, 2, 3]) |true |true |EQUAL |


#### SSE predicate (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| ([Lint) -> int |arrayMax([1, 2, 3]) |true |true |EQUAL |


#### SSE projection (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| ([Lint) -> int |arrayMax([1, 2, 3]) |3 |3 |EQUAL |


#### SSE projection (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| ([Lint) -> int |arrayMax([1, 2, 3]) |3 |3 |EQUAL |


