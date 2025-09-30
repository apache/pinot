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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ValueAggregatorBackwardCompatTest {

  @Test
  public void testSum_twoArgApplyAndInitial_withoutSourceType() {
    SumValueAggregator agg = new SumValueAggregator();

    // Legacy initial (no source type)
    assertEquals(agg.getInitialAggregatedValue(null), 0.0, 0.0);
    assertEquals(agg.getInitialAggregatedValue("2.5"), 2.5, 0.0);
    assertEquals(agg.getInitialAggregatedValue(3), 3.0, 0.0);

    // Legacy apply (two-arg)
    double after = agg.applyRawValue(1.0, "2.5");
    assertEquals(after, 3.5, 0.0);
    after = agg.applyRawValue(after, 4);
    assertEquals(after, 7.5, 0.0);
  }

  @Test
  public void testMinMax_twoArgApplyAndInitial_withoutSourceType() {
    MinValueAggregator minAgg = new MinValueAggregator();
    MaxValueAggregator maxAgg = new MaxValueAggregator();

    // Legacy initial
    assertEquals(minAgg.getInitialAggregatedValue("10.0"), 10.0, 0.0);
    assertEquals(maxAgg.getInitialAggregatedValue("10.0"), 10.0, 0.0);

    // Legacy apply
    double minAfter = minAgg.applyRawValue(5.0, 3);
    assertEquals(minAfter, 3.0, 0.0);
    double maxAfter = maxAgg.applyRawValue(5.0, 7.0);
    assertEquals(maxAfter, 7.0, 0.0);
  }

  @Test
  public void testAvg_twoArgApplyAndInitial_withoutSourceType() {
    AvgValueAggregator agg = new AvgValueAggregator();

    AvgPair initial = agg.getInitialAggregatedValue("4.0");
    assertEquals(initial.getSum(), 4.0, 0.0);
    assertEquals(initial.getCount(), 1L);

    AvgPair applied = agg.applyRawValue(new AvgPair(4.0, 1L), 2);
    assertEquals(applied.getSum(), 6.0, 0.0);
    assertEquals(applied.getCount(), 2L);
  }

  @Test
  public void testMinMaxRange_twoArgApplyAndInitial_withoutSourceType() {
    MinMaxRangeValueAggregator agg = new MinMaxRangeValueAggregator();

    var initial = agg.getInitialAggregatedValue("5.0");
    assertEquals(initial.getMin(), 5.0, 0.0);
    assertEquals(initial.getMax(), 5.0, 0.0);

    var applied = agg.applyRawValue(initial, 7.0);
    assertEquals(applied.getMin(), 5.0, 0.0);
    assertEquals(applied.getMax(), 7.0, 0.0);
  }

  @Test
  public void testSumPrecision_twoArgApplyAndInitial_withoutSourceType() {
    SumPrecisionValueAggregator agg = new SumPrecisionValueAggregator(java.util.List.of());

    BigDecimal initial = agg.getInitialAggregatedValue("1.5");
    assertEquals(initial, new BigDecimal("1.5"));

    BigDecimal after = agg.applyRawValue(initial, 2);
    assertEquals(after, new BigDecimal("3.5"));
  }

  @Test
  public void testCount_twoArgApplyAndInitial_withoutSourceType() {
    CountValueAggregator agg = new CountValueAggregator();

    assertEquals(agg.getInitialAggregatedValue(null).longValue(), 0L);
    assertEquals(agg.getInitialAggregatedValue("x").longValue(), 1L);

    long after = agg.applyRawValue(5L, "y");
    assertEquals(after, 6L);
  }
}


