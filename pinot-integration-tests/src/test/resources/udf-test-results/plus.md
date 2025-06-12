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

| Scenario | Semantic |
|----------|----------|
| Ingestion time transformer | NUMBER_AS_DOUBLE |
| MSE intermediate stage (with null handling) | BIG_DECIMAL_AS_DOUBLE |
| MSE intermediate stage (without null handling) | BIG_DECIMAL_AS_DOUBLE |
| SSE predicate (with null handling) | EQUAL |
| SSE predicate (without null handling) | EQUAL |
| SSE projection (with null handling) | NUMBER_AS_DOUBLE |
| SSE projection (without null handling) | NUMBER_AS_DOUBLE |
### Details

#### Ingestion time transformer


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (big_decimal, big_decimal) -> big_decimal |(1.0 + 2.0) |3.0 (BigDecimal) |3.0 (Double) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal, big_decimal) -> big_decimal |(1.0 + NULL) |NULL |NULL |EQUAL |
| (double, double) -> double |(1.0 + 2.0) |3.0 |3.0 |EQUAL |
| (double, double) -> double |(1.0 + NULL) |NULL |NULL |EQUAL |
| (float, float) -> float |(1.0 + NULL) |NULL |NULL |EQUAL |
| (float, float) -> float |(1.0 + 2.0) |3.0 (Float) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (int, int) -> int |(1 + NULL) |NULL |NULL |EQUAL |
| (int, int) -> int |(1 + 2) |3 (Integer) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (long, long) -> long |(1 + 2) |3 (Long) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (long, long) -> long |(1 + NULL) |NULL |NULL |EQUAL |


#### MSE intermediate stage (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (big_decimal, big_decimal) -> big_decimal |(1.0 + 2.0) |3.0 (BigDecimal) |3.0 (Double) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal, big_decimal) -> big_decimal |(1.0 + NULL) |NULL |NULL |EQUAL |
| (double, double) -> double |(1.0 + 2.0) |3.0 |3.0 |EQUAL |
| (double, double) -> double |(1.0 + NULL) |NULL |NULL |EQUAL |
| (float, float) -> float |(1.0 + NULL) |NULL |NULL |EQUAL |
| (float, float) -> float |(1.0 + 2.0) |3.0 |3.0 |EQUAL |
| (int, int) -> int |(1 + NULL) |NULL |NULL |EQUAL |
| (int, int) -> int |(1 + 2) |3 |3 |EQUAL |
| (long, long) -> long |(1 + 2) |3 |3 |EQUAL |
| (long, long) -> long |(1 + NULL) |NULL |NULL |EQUAL |


#### MSE intermediate stage (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (big_decimal, big_decimal) -> big_decimal |(1.0 + 2.0) |3.0 (BigDecimal) |3.0 (Double) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal, big_decimal) -> big_decimal |(1.0 + NULL) |1.0 (BigDecimal) |1.0 (Double) |BIG_DECIMAL_AS_DOUBLE |
| (double, double) -> double |(1.0 + 2.0) |3.0 |3.0 |EQUAL |
| (double, double) -> double |(1.0 + NULL) |1.0 |1.0 |EQUAL |
| (float, float) -> float |(1.0 + NULL) |1.0 |1.0 |EQUAL |
| (float, float) -> float |(1.0 + 2.0) |3.0 |3.0 |EQUAL |
| (int, int) -> int |(1 + NULL) |1 |1 |EQUAL |
| (int, int) -> int |(1 + 2) |3 |3 |EQUAL |
| (long, long) -> long |(1 + 2) |3 |3 |EQUAL |
| (long, long) -> long |(1 + NULL) |1 |1 |EQUAL |


#### SSE predicate (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (big_decimal, big_decimal) -> big_decimal |(1.0 + 2.0) |true |true |EQUAL |
| (big_decimal, big_decimal) -> big_decimal |(1.0 + NULL) |true |true |EQUAL |
| (double, double) -> double |(1.0 + 2.0) |true |true |EQUAL |
| (double, double) -> double |(1.0 + NULL) |true |true |EQUAL |
| (float, float) -> float |(1.0 + NULL) |true |true |EQUAL |
| (float, float) -> float |(1.0 + 2.0) |true |true |EQUAL |
| (int, int) -> int |(1 + NULL) |true |true |EQUAL |
| (int, int) -> int |(1 + 2) |true |true |EQUAL |
| (long, long) -> long |(1 + 2) |true |true |EQUAL |
| (long, long) -> long |(1 + NULL) |true |true |EQUAL |


#### SSE predicate (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (big_decimal, big_decimal) -> big_decimal |(1.0 + 2.0) |true |true |EQUAL |
| (big_decimal, big_decimal) -> big_decimal |(1.0 + NULL) |true |true |EQUAL |
| (double, double) -> double |(1.0 + 2.0) |true |true |EQUAL |
| (double, double) -> double |(1.0 + NULL) |true |true |EQUAL |
| (float, float) -> float |(1.0 + NULL) |true |true |EQUAL |
| (float, float) -> float |(1.0 + 2.0) |true |true |EQUAL |
| (int, int) -> int |(1 + NULL) |true |true |EQUAL |
| (int, int) -> int |(1 + 2) |true |true |EQUAL |
| (long, long) -> long |(1 + 2) |true |true |EQUAL |
| (long, long) -> long |(1 + NULL) |true |true |EQUAL |


#### SSE projection (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (big_decimal, big_decimal) -> big_decimal |(1.0 + 2.0) |3.0 (BigDecimal) |3.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal, big_decimal) -> big_decimal |(1.0 + NULL) |NULL |NULL |EQUAL |
| (double, double) -> double |(1.0 + 2.0) |3.0 |3.0 |EQUAL |
| (double, double) -> double |(1.0 + NULL) |NULL |NULL |EQUAL |
| (float, float) -> float |(1.0 + NULL) |NULL |NULL |EQUAL |
| (float, float) -> float |(1.0 + 2.0) |3.0 (Float) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (int, int) -> int |(1 + NULL) |NULL |NULL |EQUAL |
| (int, int) -> int |(1 + 2) |3 (Integer) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (long, long) -> long |(1 + 2) |3 (Long) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (long, long) -> long |(1 + NULL) |NULL |NULL |EQUAL |


#### SSE projection (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (big_decimal, big_decimal) -> big_decimal |(1.0 + 2.0) |3.0 (BigDecimal) |3.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal, big_decimal) -> big_decimal |(1.0 + NULL) |1.0 (BigDecimal) |1.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (double, double) -> double |(1.0 + 2.0) |3.0 |3.0 |EQUAL |
| (double, double) -> double |(1.0 + NULL) |1.0 |1.0 |EQUAL |
| (float, float) -> float |(1.0 + NULL) |1.0 (Float) |1.0 (Double) |NUMBER_AS_DOUBLE |
| (float, float) -> float |(1.0 + 2.0) |3.0 (Float) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (int, int) -> int |(1 + NULL) |1 (Integer) |1.0 (Double) |NUMBER_AS_DOUBLE |
| (int, int) -> int |(1 + 2) |3 (Integer) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (long, long) -> long |(1 + 2) |3 (Long) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (long, long) -> long |(1 + NULL) |1 (Long) |1.0 (Double) |NUMBER_AS_DOUBLE |


