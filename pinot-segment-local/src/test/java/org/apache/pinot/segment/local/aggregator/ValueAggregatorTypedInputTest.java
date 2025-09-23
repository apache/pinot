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
package org.apache.pinot.segment.local.aggregator;

import java.math.BigDecimal;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ValueAggregatorTypedInputTest {

  @Test
  public void testSumValueAggregator_initialAndApply_withSourceType() {
    SumValueAggregator agg = new SumValueAggregator();

    // initial
    assertEquals(agg.getInitialAggregatedValue(null, DataType.STRING), 0.0, 0.0);
    assertEquals(agg.getInitialAggregatedValue("3.5", DataType.STRING), 3.5, 0.0);
    assertEquals(agg.getInitialAggregatedValue(7, DataType.INT), 7.0, 0.0);

    // apply
    double after = agg.applyRawValue(1.0, "2.25", DataType.STRING);
    assertEquals(after, 3.25, 0.0);
    after = agg.applyRawValue(1.0, 2, DataType.INT);
    assertEquals(after, 3.0, 0.0);
  }

  @Test
  public void testMinMaxAggregators_initial_withSourceType() {
    MinValueAggregator minAgg = new MinValueAggregator();
    MaxValueAggregator maxAgg = new MaxValueAggregator();

    assertEquals(minAgg.getInitialAggregatedValue("2", DataType.STRING), 2.0, 0.0);
    assertEquals(maxAgg.getInitialAggregatedValue("2", DataType.STRING), 2.0, 0.0);

    assertEquals(minAgg.getInitialAggregatedValue(5L, DataType.LONG), 5.0, 0.0);
    assertEquals(maxAgg.getInitialAggregatedValue(5L, DataType.LONG), 5.0, 0.0);

    // apply
    double minAfter = minAgg.applyRawValue(10.0, 3, DataType.INT);
    assertEquals(minAfter, 3.0, 0.0);
    double maxAfter = maxAgg.applyRawValue(1.0, 3.5, DataType.DOUBLE);
    assertEquals(maxAfter, 3.5, 0.0);
  }

  @Test
  public void testAvgValueAggregator_initialAndApply_withSourceType() {
    AvgValueAggregator agg = new AvgValueAggregator();

    AvgPair initial = agg.getInitialAggregatedValue("4.0", DataType.STRING);
    assertEquals(initial.getSum(), 4.0, 0.0);
    assertEquals(initial.getCount(), 1L);

    AvgPair applied = agg.applyRawValue(new AvgPair(4.0, 1L), "2.0", DataType.STRING);
    assertEquals(applied.getSum(), 6.0, 0.0);
    assertEquals(applied.getCount(), 2L);
  }

  @Test
  public void testMinMaxRangeValueAggregator_initialAndApply_withSourceType() {
    MinMaxRangeValueAggregator agg = new MinMaxRangeValueAggregator();

    var initial = agg.getInitialAggregatedValue("5.0", DataType.STRING);
    assertEquals(initial.getMin(), 5.0, 0.0);
    assertEquals(initial.getMax(), 5.0, 0.0);

    var applied = agg.applyRawValue(initial, 7.0, DataType.DOUBLE);
    assertEquals(applied.getMin(), 5.0, 0.0);
    assertEquals(applied.getMax(), 7.0, 0.0);
  }

  @Test
  public void testSumPrecisionValueAggregator_initialAndApply_withSourceType() {
    SumPrecisionValueAggregator agg = new SumPrecisionValueAggregator(java.util.List.of());

    BigDecimal initial = agg.getInitialAggregatedValue("12.3", DataType.STRING);
    assertEquals(initial, new BigDecimal("12.3"));

    BigDecimal after = agg.applyRawValue(initial, 7L, DataType.LONG);
    assertEquals(after, new BigDecimal("19.3"));
  }

  @Test
  public void testCountValueAggregator_initialAndApply_withSourceType() {
    CountValueAggregator agg = new CountValueAggregator();

    // initial
    assertEquals(agg.getInitialAggregatedValue(null, DataType.INT).longValue(), 0L);
    assertEquals(agg.getInitialAggregatedValue(1, DataType.INT).longValue(), 1L);
    assertEquals(agg.getInitialAggregatedValue("x", DataType.STRING).longValue(), 1L);

    // apply increments regardless of data type
    long after = agg.applyRawValue(5L, 123, DataType.INT);
    assertEquals(after, 6L);
    after = agg.applyRawValue(after, "y", DataType.STRING);
    assertEquals(after, 7L);
  }
}


