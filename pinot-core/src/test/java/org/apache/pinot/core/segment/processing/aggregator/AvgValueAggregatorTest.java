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
package org.apache.pinot.core.segment.processing.aggregator;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class AvgValueAggregatorTest {

  private final Map<String, String> _functionParameters = new HashMap<>();
  private AvgValueAggregator _aggregator;

  @BeforeMethod
  public void setUp() {
    _aggregator = new AvgValueAggregator();
  }

  private static byte[] serialize(double sum, long count) {
    return ObjectSerDeUtils.AVG_PAIR_SER_DE.serialize(new AvgPair(sum, count));
  }

  private AvgPair aggregate(byte[] value1, byte[] value2) {
    byte[] result = (byte[]) _aggregator.aggregate(value1, value2, _functionParameters);
    return ObjectSerDeUtils.AVG_PAIR_SER_DE.deserialize(result);
  }

  @Test
  public void testAggregateAddsSumAndCount() {
    // (sum=30, count=2) avg 15 merged with (sum=20, count=4) avg 5 -> total sum 50, total count 6, avg 8.33...
    AvgPair merged = aggregate(serialize(30.0, 2L), serialize(20.0, 4L));
    assertEquals(merged.getSum(), 50.0);
    assertEquals(merged.getCount(), 6L);
  }

  @Test
  public void testTwoLevelRollupPreservesSumAndCount() {
    // The average-of-averages trap: a second rollup pass over already-rolled-up rows must keep the
    // running sum/count, not re-average the level-1 averages.
    // Level-1 group A: 50 rows of value 2 -> (sum=100, count=50), avg 2
    // Level-1 group B: 50 rows of value 4 -> (sum=200, count=50), avg 4
    byte[] level1A = serialize(100.0, 50L);
    byte[] level1B = serialize(200.0, 50L);

    AvgPair level2 = aggregate(level1A, level1B);
    assertEquals(level2.getSum(), 300.0);
    assertEquals(level2.getCount(), 100L);
    // Correct overall average is 3.0 (300/100), NOT 3.0 by accident of (2+4)/2 — verify via the pair.
    assertEquals(level2.getSum() / level2.getCount(), 3.0);
  }

  @Test
  public void testAggregateWithBothEmptyBytes() {
    byte[] result = (byte[]) _aggregator.aggregate(new byte[0], new byte[0], _functionParameters);
    // Both empty (default null value for BYTES columns) -> treated as missing, return empty bytes
    assertEquals(result.length, 0);
  }

  @Test
  public void testAggregateWithFirstEmptyBytes() {
    byte[] value2 = serialize(42.0, 7L);
    byte[] result = (byte[]) _aggregator.aggregate(new byte[0], value2, _functionParameters);
    // Should return the non-empty side as-is
    assertEquals(result, value2);
  }

  @Test
  public void testAggregateWithSecondEmptyBytes() {
    byte[] value1 = serialize(42.0, 7L);
    byte[] result = (byte[]) _aggregator.aggregate(value1, new byte[0], _functionParameters);
    // Should return the non-empty side as-is
    assertEquals(result, value1);
  }

  @Test(expectedExceptions = java.nio.BufferUnderflowException.class)
  public void testAggregateRejectsMalformedBytes() {
    // A non-empty but malformed (too short) AvgPair buffer must fail loudly rather than silently corrupt.
    _aggregator.aggregate(serialize(1.0, 1L), new byte[]{1, 2, 3}, _functionParameters);
  }

  @Test
  public void testFactoryReturnsAvgAggregator() {
    ValueAggregator aggregator = ValueAggregatorFactory.getValueAggregator(AggregationFunctionType.AVG, DataType.BYTES);
    assertTrue(aggregator instanceof AvgValueAggregator);
  }
}
