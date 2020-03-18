/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.aggregation.function;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class AggregationFunctionUtilsTest {

  @Test
  public void testFormatValue() {
    double value = Long.MAX_VALUE;
    assertEquals(AggregationFunctionUtils.formatValue(value), "9223372036854775807.00000");

    value = Long.MIN_VALUE;
    assertEquals(AggregationFunctionUtils.formatValue(value), "-9223372036854775808.00000");

    value = 1e30;
    assertEquals(AggregationFunctionUtils.formatValue(value), "1000000000000000000000000000000.00000");

    value = -1e30;
    assertEquals(AggregationFunctionUtils.formatValue(value), "-1000000000000000000000000000000.00000");

    value = 1e-3;
    assertEquals(AggregationFunctionUtils.formatValue(value), "0.00100");

    value = -1e-3;
    assertEquals(AggregationFunctionUtils.formatValue(value), "-0.00100");

    value = 1e-10;
    assertEquals(AggregationFunctionUtils.formatValue(value), "0.00000");

    value = -1e-10;
    assertEquals(AggregationFunctionUtils.formatValue(value), "-0.00000");

    value = 123.456789;
    assertEquals(AggregationFunctionUtils.formatValue(value), "123.45679");

    value = Double.POSITIVE_INFINITY;
    assertEquals(AggregationFunctionUtils.formatValue(value), "Infinity");

    value = Double.NEGATIVE_INFINITY;
    assertEquals(AggregationFunctionUtils.formatValue(value), "-Infinity");

    value = Double.NaN;
    assertEquals(AggregationFunctionUtils.formatValue(value), "NaN");
  }
}
