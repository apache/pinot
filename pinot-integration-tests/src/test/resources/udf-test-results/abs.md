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

## abs

### Description

Returns the absolute value of a numeric input.
### Summary

| Scenario | Semantic |
|----------|----------|
| Ingestion time transformer | NUMBER_AS_DOUBLE |
| MSE intermediate stage (with null handling) | BIG_DECIMAL_AS_DOUBLE |
| MSE intermediate stage (without null handling) | BIG_DECIMAL_AS_DOUBLE |
| SSE predicate (with null handling) | EQUAL |
| SSE predicate (without null handling) | EQUAL |
| SSE projection (with null handling) | NUMBER_AS_DOUBLE |
| SSE projection (without null handling) | NUMBER_AS_DOUBLE |
### Signatures

#### abs(arg0: long) -> long

| Parameter | Type | Description |
|-----------|------|-------------|
| arg0 | long |  |
#### abs(arg0: double) -> double

| Parameter | Type | Description |
|-----------|------|-------------|
| arg0 | double |  |
#### abs(arg0: big_decimal) -> big_decimal

| Parameter | Type | Description |
|-----------|------|-------------|
| arg0 | big_decimal |  |
#### abs(arg0: float) -> float

| Parameter | Type | Description |
|-----------|------|-------------|
| arg0 | float |  |
#### abs(arg0: int) -> int

| Parameter | Type | Description |
|-----------|------|-------------|
| arg0 | int |  |
### Scenarios

#### Ingestion time transformer


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal) -> big_decimal |abs(-3.0) |3.0 (BigDecimal) |3.0 (Double) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: big_decimal) -> big_decimal |abs(NULL) |NULL |NULL |EQUAL |
| (arg0: big_decimal) -> big_decimal |abs(0.0) |0.0 (BigDecimal) |0.0 (Double) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: big_decimal) -> big_decimal |abs(5.0) |5.0 (BigDecimal) |5.0 (Double) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: double) -> double |abs(0.0) |0.0 |0.0 |EQUAL |
| (arg0: double) -> double |abs(5.0) |5.0 |5.0 |EQUAL |
| (arg0: double) -> double |abs(-3.0) |3.0 |3.0 |EQUAL |
| (arg0: double) -> double |abs(NULL) |NULL |NULL |EQUAL |
| (arg0: float) -> float |abs(0.0) |0.0 (Float) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: float) -> float |abs(-3.0) |3.0 (Float) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: float) -> float |abs(5.0) |5.0 (Float) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: float) -> float |abs(NULL) |NULL |NULL |EQUAL |
| (arg0: int) -> int |abs(5) |5 (Integer) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int) -> int |abs(-3) |3 (Integer) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int) -> int |abs(NULL) |NULL |NULL |EQUAL |
| (arg0: int) -> int |abs(0) |0 (Integer) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long) -> long |abs(0) |0 (Long) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long) -> long |abs(5) |5 (Long) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long) -> long |abs(-3) |3 (Long) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long) -> long |abs(NULL) |NULL |NULL |EQUAL |


#### MSE intermediate stage (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal) -> big_decimal |abs(-3.0) |3.0 (BigDecimal) |3.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: big_decimal) -> big_decimal |abs(0.0) |0.0 (BigDecimal) |0.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: big_decimal) -> big_decimal |abs(NULL) |NULL |NULL |EQUAL |
| (arg0: big_decimal) -> big_decimal |abs(5.0) |5.0 (BigDecimal) |5.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: double) -> double |abs(-3.0) |3.0 |3.0 |EQUAL |
| (arg0: double) -> double |abs(0.0) |0.0 |0.0 |EQUAL |
| (arg0: double) -> double |abs(NULL) |NULL |NULL |EQUAL |
| (arg0: double) -> double |abs(5.0) |5.0 |5.0 |EQUAL |
| (arg0: float) -> float |abs(0.0) |0.0 |0.0 |EQUAL |
| (arg0: float) -> float |abs(NULL) |NULL |NULL |EQUAL |
| (arg0: float) -> float |abs(-3.0) |3.0 |3.0 |EQUAL |
| (arg0: float) -> float |abs(5.0) |5.0 |5.0 |EQUAL |
| (arg0: int) -> int |abs(5) |5 |5 |EQUAL |
| (arg0: int) -> int |abs(NULL) |NULL |NULL |EQUAL |
| (arg0: int) -> int |abs(-3) |3 |3 |EQUAL |
| (arg0: int) -> int |abs(0) |0 |0 |EQUAL |
| (arg0: long) -> long |abs(0) |0 |0 |EQUAL |
| (arg0: long) -> long |abs(5) |5 |5 |EQUAL |
| (arg0: long) -> long |abs(-3) |3 |3 |EQUAL |
| (arg0: long) -> long |abs(NULL) |NULL |NULL |EQUAL |


#### MSE intermediate stage (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal) -> big_decimal |abs(-3.0) |3.0 (BigDecimal) |3.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: big_decimal) -> big_decimal |abs(0.0) |0.0 (BigDecimal) |0.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: big_decimal) -> big_decimal |abs(NULL) |0.0 (BigDecimal) |0.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: big_decimal) -> big_decimal |abs(5.0) |5.0 (BigDecimal) |5.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: double) -> double |abs(-3.0) |3.0 |3.0 |EQUAL |
| (arg0: double) -> double |abs(0.0) |0.0 |0.0 |EQUAL |
| (arg0: double) -> double |abs(NULL) |0.0 |0.0 |EQUAL |
| (arg0: double) -> double |abs(5.0) |5.0 |5.0 |EQUAL |
| (arg0: float) -> float |abs(0.0) |0.0 |0.0 |EQUAL |
| (arg0: float) -> float |abs(NULL) |0.0 |0.0 |EQUAL |
| (arg0: float) -> float |abs(-3.0) |3.0 |3.0 |EQUAL |
| (arg0: float) -> float |abs(5.0) |5.0 |5.0 |EQUAL |
| (arg0: int) -> int |abs(5) |5 |5 |EQUAL |
| (arg0: int) -> int |abs(NULL) |0 |0 |EQUAL |
| (arg0: int) -> int |abs(-3) |3 |3 |EQUAL |
| (arg0: int) -> int |abs(0) |0 |0 |EQUAL |
| (arg0: long) -> long |abs(0) |0 |0 |EQUAL |
| (arg0: long) -> long |abs(5) |5 |5 |EQUAL |
| (arg0: long) -> long |abs(-3) |3 |3 |EQUAL |
| (arg0: long) -> long |abs(NULL) |0 |0 |EQUAL |


#### SSE predicate (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal) -> big_decimal |abs(-3.0) |true |true |EQUAL |
| (arg0: big_decimal) -> big_decimal |abs(NULL) |true |true |EQUAL |
| (arg0: big_decimal) -> big_decimal |abs(0.0) |true |true |EQUAL |
| (arg0: big_decimal) -> big_decimal |abs(5.0) |true |true |EQUAL |
| (arg0: double) -> double |abs(0.0) |true |true |EQUAL |
| (arg0: double) -> double |abs(5.0) |true |true |EQUAL |
| (arg0: double) -> double |abs(-3.0) |true |true |EQUAL |
| (arg0: double) -> double |abs(NULL) |true |true |EQUAL |
| (arg0: float) -> float |abs(0.0) |true |true |EQUAL |
| (arg0: float) -> float |abs(-3.0) |true |true |EQUAL |
| (arg0: float) -> float |abs(5.0) |true |true |EQUAL |
| (arg0: float) -> float |abs(NULL) |true |true |EQUAL |
| (arg0: int) -> int |abs(5) |true |true |EQUAL |
| (arg0: int) -> int |abs(-3) |true |true |EQUAL |
| (arg0: int) -> int |abs(NULL) |true |true |EQUAL |
| (arg0: int) -> int |abs(0) |true |true |EQUAL |
| (arg0: long) -> long |abs(0) |true |true |EQUAL |
| (arg0: long) -> long |abs(5) |true |true |EQUAL |
| (arg0: long) -> long |abs(-3) |true |true |EQUAL |
| (arg0: long) -> long |abs(NULL) |true |true |EQUAL |


#### SSE predicate (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal) -> big_decimal |abs(-3.0) |true |true |EQUAL |
| (arg0: big_decimal) -> big_decimal |abs(NULL) |true |true |EQUAL |
| (arg0: big_decimal) -> big_decimal |abs(0.0) |true |true |EQUAL |
| (arg0: big_decimal) -> big_decimal |abs(5.0) |true |true |EQUAL |
| (arg0: double) -> double |abs(0.0) |true |true |EQUAL |
| (arg0: double) -> double |abs(5.0) |true |true |EQUAL |
| (arg0: double) -> double |abs(-3.0) |true |true |EQUAL |
| (arg0: double) -> double |abs(NULL) |true |true |EQUAL |
| (arg0: float) -> float |abs(0.0) |true |true |EQUAL |
| (arg0: float) -> float |abs(-3.0) |true |true |EQUAL |
| (arg0: float) -> float |abs(5.0) |true |true |EQUAL |
| (arg0: float) -> float |abs(NULL) |true |true |EQUAL |
| (arg0: int) -> int |abs(5) |true |true |EQUAL |
| (arg0: int) -> int |abs(-3) |true |true |EQUAL |
| (arg0: int) -> int |abs(NULL) |true |true |EQUAL |
| (arg0: int) -> int |abs(0) |true |true |EQUAL |
| (arg0: long) -> long |abs(0) |true |true |EQUAL |
| (arg0: long) -> long |abs(5) |true |true |EQUAL |
| (arg0: long) -> long |abs(-3) |true |true |EQUAL |
| (arg0: long) -> long |abs(NULL) |true |true |EQUAL |


#### SSE projection (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal) -> big_decimal |abs(-3.0) |3.0 (BigDecimal) |3 (String) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: big_decimal) -> big_decimal |abs(0.0) |0.0 (BigDecimal) |0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: big_decimal) -> big_decimal |abs(NULL) |NULL |NULL |EQUAL |
| (arg0: big_decimal) -> big_decimal |abs(5.0) |5.0 (BigDecimal) |5 (String) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: double) -> double |abs(-3.0) |3.0 |3.0 |EQUAL |
| (arg0: double) -> double |abs(0.0) |0.0 |0.0 |EQUAL |
| (arg0: double) -> double |abs(NULL) |NULL |NULL |EQUAL |
| (arg0: double) -> double |abs(5.0) |5.0 |5.0 |EQUAL |
| (arg0: float) -> float |abs(0.0) |0.0 (Float) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: float) -> float |abs(NULL) |NULL |NULL |EQUAL |
| (arg0: float) -> float |abs(-3.0) |3.0 (Float) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: float) -> float |abs(5.0) |5.0 (Float) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int) -> int |abs(5) |5 (Integer) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int) -> int |abs(NULL) |NULL |NULL |EQUAL |
| (arg0: int) -> int |abs(-3) |3 (Integer) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int) -> int |abs(0) |0 (Integer) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long) -> long |abs(0) |0 (Long) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long) -> long |abs(5) |5 (Long) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long) -> long |abs(-3) |3 (Long) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long) -> long |abs(NULL) |NULL |NULL |EQUAL |


#### SSE projection (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (arg0: big_decimal) -> big_decimal |abs(-3.0) |3.0 (BigDecimal) |3 (String) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: big_decimal) -> big_decimal |abs(0.0) |0.0 (BigDecimal) |0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: big_decimal) -> big_decimal |abs(NULL) |0.0 (BigDecimal) |0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: big_decimal) -> big_decimal |abs(5.0) |5.0 (BigDecimal) |5 (String) |BIG_DECIMAL_AS_DOUBLE |
| (arg0: double) -> double |abs(-3.0) |3.0 |3.0 |EQUAL |
| (arg0: double) -> double |abs(0.0) |0.0 |0.0 |EQUAL |
| (arg0: double) -> double |abs(NULL) |0.0 |0.0 |EQUAL |
| (arg0: double) -> double |abs(5.0) |5.0 |5.0 |EQUAL |
| (arg0: float) -> float |abs(0.0) |0.0 (Float) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: float) -> float |abs(NULL) |0.0 (Float) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: float) -> float |abs(-3.0) |3.0 (Float) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: float) -> float |abs(5.0) |5.0 (Float) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int) -> int |abs(5) |5 (Integer) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int) -> int |abs(NULL) |0 (Integer) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int) -> int |abs(-3) |3 (Integer) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: int) -> int |abs(0) |0 (Integer) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long) -> long |abs(0) |0 (Long) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long) -> long |abs(5) |5 (Long) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long) -> long |abs(-3) |3 (Long) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (arg0: long) -> long |abs(NULL) |0 (Long) |0.0 (Double) |NUMBER_AS_DOUBLE |


