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

## toDateTime

### Description

Converts epoch millis to a DateTime string represented by the given pattern. Optionally, a timezone can be provided.
### Summary

The UDF toDateTime is supported in all scenarios with at least EQUAL semantic.

### Signatures

#### toDateTime(mills: long, format: string) -> string

| Parameter | Type | Description |
|-----------|------|-------------|
| mills | long | A long value representing epoch millis, e.g., 1577836800000L for 2020-01-01T00:00:00Z |
| format | string | A string literal representing the date format, e.g., 'yyyy-MM-dd'T'HH:mm:ss'Z' or 'yyyy-MM-dd' |
### Scenarios

#### Ingestion time transformer


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (mills: long, format: string) -> string |toDateTime(1577836800000, 'yyyy-MM-dd''T''HH:mm:ss''Z''') |2020-01-01T00:00:00Z |2020-01-01T00:00:00Z |EQUAL |
| (mills: long, format: string) -> string |toDateTime(1577836800000, 'yyyy-MM-dd') |2020-01-01 |2020-01-01 |EQUAL |


#### MSE intermediate stage (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (mills: long, format: string) -> string |toDateTime(1577836800000, 'yyyy-MM-dd') |2020-01-01 |2020-01-01 |EQUAL |
| (mills: long, format: string) -> string |toDateTime(1577836800000, 'yyyy-MM-dd''T''HH:mm:ss''Z''') |2020-01-01T00:00:00Z |2020-01-01T00:00:00Z |EQUAL |


#### MSE intermediate stage (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (mills: long, format: string) -> string |toDateTime(1577836800000, 'yyyy-MM-dd') |2020-01-01 |2020-01-01 |EQUAL |
| (mills: long, format: string) -> string |toDateTime(1577836800000, 'yyyy-MM-dd''T''HH:mm:ss''Z''') |2020-01-01T00:00:00Z |2020-01-01T00:00:00Z |EQUAL |


#### SSE predicate (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (mills: long, format: string) -> string |toDateTime(1577836800000, 'yyyy-MM-dd''T''HH:mm:ss''Z''') |true |true |EQUAL |
| (mills: long, format: string) -> string |toDateTime(1577836800000, 'yyyy-MM-dd') |true |true |EQUAL |


#### SSE predicate (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (mills: long, format: string) -> string |toDateTime(1577836800000, 'yyyy-MM-dd''T''HH:mm:ss''Z''') |true |true |EQUAL |
| (mills: long, format: string) -> string |toDateTime(1577836800000, 'yyyy-MM-dd') |true |true |EQUAL |


#### SSE projection (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (mills: long, format: string) -> string |toDateTime(1577836800000, 'yyyy-MM-dd') |2020-01-01 |2020-01-01 |EQUAL |
| (mills: long, format: string) -> string |toDateTime(1577836800000, 'yyyy-MM-dd''T''HH:mm:ss''Z''') |2020-01-01T00:00:00Z |2020-01-01T00:00:00Z |EQUAL |


#### SSE projection (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (mills: long, format: string) -> string |toDateTime(1577836800000, 'yyyy-MM-dd') |2020-01-01 |2020-01-01 |EQUAL |
| (mills: long, format: string) -> string |toDateTime(1577836800000, 'yyyy-MM-dd''T''HH:mm:ss''Z''') |2020-01-01T00:00:00Z |2020-01-01T00:00:00Z |EQUAL |


