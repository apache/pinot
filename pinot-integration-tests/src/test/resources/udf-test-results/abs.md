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
| (big_decimal) -> big_decimal |abs(-3.0) |3.0 (BigDecimal) |3.0 (Double) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal) -> big_decimal |abs(NULL) |NULL |NULL |EQUAL |
| (big_decimal) -> big_decimal |abs(0.0) |0.0 (BigDecimal) |0.0 (Double) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal) -> big_decimal |abs(5.0) |5.0 (BigDecimal) |5.0 (Double) |BIG_DECIMAL_AS_DOUBLE |
| (double) -> double |abs(0.0) |0.0 |0.0 |EQUAL |
| (double) -> double |abs(5.0) |5.0 |5.0 |EQUAL |
| (double) -> double |abs(-3.0) |3.0 |3.0 |EQUAL |
| (double) -> double |abs(NULL) |NULL |NULL |EQUAL |
| (float) -> float |abs(0.0) |0.0 (Float) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (float) -> float |abs(-3.0) |3.0 (Float) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (float) -> float |abs(5.0) |5.0 (Float) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (float) -> float |abs(NULL) |NULL |NULL |EQUAL |
| (int) -> int |abs(5) |5 (Integer) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (int) -> int |abs(-3) |3 (Integer) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (int) -> int |abs(NULL) |NULL |NULL |EQUAL |
| (int) -> int |abs(0) |0 (Integer) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (long) -> long |abs(0) |0 (Long) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (long) -> long |abs(5) |5 (Long) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (long) -> long |abs(-3) |3 (Long) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (long) -> long |abs(NULL) |NULL |NULL |EQUAL |


#### MSE intermediate stage (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (big_decimal) -> big_decimal |abs(-3.0) |3.0 (BigDecimal) |3.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal) -> big_decimal |abs(NULL) |NULL |NULL |EQUAL |
| (big_decimal) -> big_decimal |abs(0.0) |0.0 (BigDecimal) |0.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal) -> big_decimal |abs(5.0) |5.0 (BigDecimal) |5.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (double) -> double |abs(0.0) |0.0 |0.0 |EQUAL |
| (double) -> double |abs(5.0) |5.0 |5.0 |EQUAL |
| (double) -> double |abs(-3.0) |3.0 |3.0 |EQUAL |
| (double) -> double |abs(NULL) |NULL |NULL |EQUAL |
| (float) -> float |abs(0.0) |0.0 |0.0 |EQUAL |
| (float) -> float |abs(-3.0) |3.0 |3.0 |EQUAL |
| (float) -> float |abs(5.0) |5.0 |5.0 |EQUAL |
| (float) -> float |abs(NULL) |NULL |NULL |EQUAL |
| (int) -> int |abs(5) |5 |5 |EQUAL |
| (int) -> int |abs(-3) |3 |3 |EQUAL |
| (int) -> int |abs(NULL) |NULL |NULL |EQUAL |
| (int) -> int |abs(0) |0 |0 |EQUAL |
| (long) -> long |abs(0) |0 |0 |EQUAL |
| (long) -> long |abs(5) |5 |5 |EQUAL |
| (long) -> long |abs(-3) |3 |3 |EQUAL |
| (long) -> long |abs(NULL) |NULL |NULL |EQUAL |


#### MSE intermediate stage (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (big_decimal) -> big_decimal |abs(-3.0) |3.0 (BigDecimal) |3.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal) -> big_decimal |abs(NULL) |0.0 (BigDecimal) |0.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal) -> big_decimal |abs(0.0) |0.0 (BigDecimal) |0.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal) -> big_decimal |abs(5.0) |5.0 (BigDecimal) |5.0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (double) -> double |abs(0.0) |0.0 |0.0 |EQUAL |
| (double) -> double |abs(5.0) |5.0 |5.0 |EQUAL |
| (double) -> double |abs(-3.0) |3.0 |3.0 |EQUAL |
| (double) -> double |abs(NULL) |0.0 |0.0 |EQUAL |
| (float) -> float |abs(0.0) |0.0 |0.0 |EQUAL |
| (float) -> float |abs(-3.0) |3.0 |3.0 |EQUAL |
| (float) -> float |abs(5.0) |5.0 |5.0 |EQUAL |
| (float) -> float |abs(NULL) |0.0 |0.0 |EQUAL |
| (int) -> int |abs(5) |5 |5 |EQUAL |
| (int) -> int |abs(-3) |3 |3 |EQUAL |
| (int) -> int |abs(NULL) |0 |0 |EQUAL |
| (int) -> int |abs(0) |0 |0 |EQUAL |
| (long) -> long |abs(0) |0 |0 |EQUAL |
| (long) -> long |abs(5) |5 |5 |EQUAL |
| (long) -> long |abs(-3) |3 |3 |EQUAL |
| (long) -> long |abs(NULL) |0 |0 |EQUAL |


#### SSE predicate (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (big_decimal) -> big_decimal |abs(-3.0) |true |true |EQUAL |
| (big_decimal) -> big_decimal |abs(NULL) |true |true |EQUAL |
| (big_decimal) -> big_decimal |abs(0.0) |true |true |EQUAL |
| (big_decimal) -> big_decimal |abs(5.0) |true |true |EQUAL |
| (double) -> double |abs(0.0) |true |true |EQUAL |
| (double) -> double |abs(5.0) |true |true |EQUAL |
| (double) -> double |abs(-3.0) |true |true |EQUAL |
| (double) -> double |abs(NULL) |true |true |EQUAL |
| (float) -> float |abs(0.0) |true |true |EQUAL |
| (float) -> float |abs(-3.0) |true |true |EQUAL |
| (float) -> float |abs(5.0) |true |true |EQUAL |
| (float) -> float |abs(NULL) |true |true |EQUAL |
| (int) -> int |abs(5) |true |true |EQUAL |
| (int) -> int |abs(-3) |true |true |EQUAL |
| (int) -> int |abs(NULL) |true |true |EQUAL |
| (int) -> int |abs(0) |true |true |EQUAL |
| (long) -> long |abs(0) |true |true |EQUAL |
| (long) -> long |abs(5) |true |true |EQUAL |
| (long) -> long |abs(-3) |true |true |EQUAL |
| (long) -> long |abs(NULL) |true |true |EQUAL |


#### SSE predicate (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (big_decimal) -> big_decimal |abs(-3.0) |true |true |EQUAL |
| (big_decimal) -> big_decimal |abs(NULL) |true |true |EQUAL |
| (big_decimal) -> big_decimal |abs(0.0) |true |true |EQUAL |
| (big_decimal) -> big_decimal |abs(5.0) |true |true |EQUAL |
| (double) -> double |abs(0.0) |true |true |EQUAL |
| (double) -> double |abs(5.0) |true |true |EQUAL |
| (double) -> double |abs(-3.0) |true |true |EQUAL |
| (double) -> double |abs(NULL) |true |true |EQUAL |
| (float) -> float |abs(0.0) |true |true |EQUAL |
| (float) -> float |abs(-3.0) |true |true |EQUAL |
| (float) -> float |abs(5.0) |true |true |EQUAL |
| (float) -> float |abs(NULL) |true |true |EQUAL |
| (int) -> int |abs(5) |true |true |EQUAL |
| (int) -> int |abs(-3) |true |true |EQUAL |
| (int) -> int |abs(NULL) |true |true |EQUAL |
| (int) -> int |abs(0) |true |true |EQUAL |
| (long) -> long |abs(0) |true |true |EQUAL |
| (long) -> long |abs(5) |true |true |EQUAL |
| (long) -> long |abs(-3) |true |true |EQUAL |
| (long) -> long |abs(NULL) |true |true |EQUAL |


#### SSE projection (with null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (big_decimal) -> big_decimal |abs(-3.0) |3.0 (BigDecimal) |3 (String) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal) -> big_decimal |abs(NULL) |NULL |NULL |EQUAL |
| (big_decimal) -> big_decimal |abs(0.0) |0.0 (BigDecimal) |0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal) -> big_decimal |abs(5.0) |5.0 (BigDecimal) |5 (String) |BIG_DECIMAL_AS_DOUBLE |
| (double) -> double |abs(0.0) |0.0 |0.0 |EQUAL |
| (double) -> double |abs(5.0) |5.0 |5.0 |EQUAL |
| (double) -> double |abs(-3.0) |3.0 |3.0 |EQUAL |
| (double) -> double |abs(NULL) |NULL |NULL |EQUAL |
| (float) -> float |abs(0.0) |0.0 (Float) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (float) -> float |abs(-3.0) |3.0 (Float) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (float) -> float |abs(5.0) |5.0 (Float) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (float) -> float |abs(NULL) |NULL |NULL |EQUAL |
| (int) -> int |abs(5) |5 (Integer) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (int) -> int |abs(-3) |3 (Integer) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (int) -> int |abs(NULL) |NULL |NULL |EQUAL |
| (int) -> int |abs(0) |0 (Integer) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (long) -> long |abs(0) |0 (Long) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (long) -> long |abs(5) |5 (Long) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (long) -> long |abs(-3) |3 (Long) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (long) -> long |abs(NULL) |NULL |NULL |EQUAL |


#### SSE projection (without null handling)


| Signature | Call | Expected result | Actual result | Comparison or Error |
|-----------|------|-----------------|---------------|---------------------|
| (big_decimal) -> big_decimal |abs(-3.0) |3.0 (BigDecimal) |3 (String) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal) -> big_decimal |abs(NULL) |0.0 (BigDecimal) |0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal) -> big_decimal |abs(0.0) |0.0 (BigDecimal) |0 (String) |BIG_DECIMAL_AS_DOUBLE |
| (big_decimal) -> big_decimal |abs(5.0) |5.0 (BigDecimal) |5 (String) |BIG_DECIMAL_AS_DOUBLE |
| (double) -> double |abs(0.0) |0.0 |0.0 |EQUAL |
| (double) -> double |abs(5.0) |5.0 |5.0 |EQUAL |
| (double) -> double |abs(-3.0) |3.0 |3.0 |EQUAL |
| (double) -> double |abs(NULL) |0.0 |0.0 |EQUAL |
| (float) -> float |abs(0.0) |0.0 (Float) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (float) -> float |abs(-3.0) |3.0 (Float) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (float) -> float |abs(5.0) |5.0 (Float) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (float) -> float |abs(NULL) |0.0 (Float) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (int) -> int |abs(5) |5 (Integer) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (int) -> int |abs(-3) |3 (Integer) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (int) -> int |abs(NULL) |0 (Integer) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (int) -> int |abs(0) |0 (Integer) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (long) -> long |abs(0) |0 (Long) |0.0 (Double) |NUMBER_AS_DOUBLE |
| (long) -> long |abs(5) |5 (Long) |5.0 (Double) |NUMBER_AS_DOUBLE |
| (long) -> long |abs(-3) |3 (Long) |3.0 (Double) |NUMBER_AS_DOUBLE |
| (long) -> long |abs(NULL) |0 (Long) |0.0 (Double) |NUMBER_AS_DOUBLE |


