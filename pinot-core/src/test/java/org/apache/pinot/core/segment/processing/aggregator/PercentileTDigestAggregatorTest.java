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

import com.tdunning.math.stats.TDigest;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class PercentileTDigestAggregatorTest {

  private PercentileTDigestAggregator _aggregator;

  @BeforeMethod
  public void setUp() {
    _aggregator = new PercentileTDigestAggregator();
  }

  @Test
  public void testAggregateWithDefaultCompression() {
    TDigest first = TDigest.createMergingDigest(100);
    for (int i = 0; i < 100; i++) {
      first.add(i);
    }
    TDigest second = TDigest.createMergingDigest(100);
    for (int i = 100; i < 200; i++) {
      second.add(i);
    }

    byte[] value1 = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(first);
    byte[] value2 = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(second);

    Map<String, String> functionParameters = new HashMap<>();
    byte[] result = (byte[]) _aggregator.aggregate(value1, value2, functionParameters);

    TDigest resultDigest = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(result);
    assertNotNull(resultDigest);
    assertEquals(resultDigest.size(), 200);
    assertEquals(resultDigest.quantile(0.5), 99.5, 1);
  }

  @Test
  public void testAggregateWithCustomCompression() {
    TDigest first = TDigest.createMergingDigest(100);
    for (int i = 0; i < 50; i++) {
      first.add(i);
    }
    TDigest second = TDigest.createMergingDigest(100);
    for (int i = 50; i < 100; i++) {
      second.add(i);
    }

    byte[] value1 = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(first);
    byte[] value2 = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(second);

    Map<String, String> functionParameters = new HashMap<>();
    functionParameters.put(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, "200");

    byte[] result = (byte[]) _aggregator.aggregate(value1, value2, functionParameters);

    TDigest resultDigest = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(result);
    assertNotNull(resultDigest);
    assertEquals(resultDigest.size(), 100);
    assertEquals(resultDigest.quantile(0.5), 49.5, 1);
  }

  @Test
  public void testAggregateWithBothEmptyBytes() {
    byte[] empty1 = new byte[0];
    byte[] empty2 = new byte[0];

    Map<String, String> functionParameters = new HashMap<>();
    byte[] result = (byte[]) _aggregator.aggregate(empty1, empty2, functionParameters);

    // Both empty — treat as missing, return empty bytes
    assertEquals(result.length, 0);
  }

  @Test
  public void testAggregateWithFirstEmptyBytes() {
    TDigest second = TDigest.createMergingDigest(100);
    for (int i = 0; i < 50; i++) {
      second.add(i);
    }
    byte[] empty = new byte[0];
    byte[] value2 = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(second);

    Map<String, String> functionParameters = new HashMap<>();
    byte[] result = (byte[]) _aggregator.aggregate(empty, value2, functionParameters);

    // Should return the non-empty side as-is
    assertEquals(result, value2);
    TDigest resultDigest = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(result);
    assertEquals(resultDigest.size(), 50);
  }

  @Test
  public void testAggregateWithSecondEmptyBytes() {
    TDigest first = TDigest.createMergingDigest(100);
    for (int i = 0; i < 50; i++) {
      first.add(i);
    }
    byte[] value1 = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(first);
    byte[] empty = new byte[0];

    Map<String, String> functionParameters = new HashMap<>();
    byte[] result = (byte[]) _aggregator.aggregate(value1, empty, functionParameters);

    // Should return the non-empty side as-is
    assertEquals(result, value1);
    TDigest resultDigest = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(result);
    assertEquals(resultDigest.size(), 50);
  }

  @Test
  public void testFactoryReturnsAggregatorForNonRawType() {
    ValueAggregator aggregator = ValueAggregatorFactory.getValueAggregator(
        AggregationFunctionType.PERCENTILETDIGEST, DataType.BYTES);
    assertTrue(aggregator instanceof PercentileTDigestAggregator);
  }

  @Test
  public void testFactoryReturnsAggregatorForRawType() {
    ValueAggregator aggregator = ValueAggregatorFactory.getValueAggregator(
        AggregationFunctionType.PERCENTILERAWTDIGEST, DataType.BYTES);
    assertTrue(aggregator instanceof PercentileTDigestAggregator);
  }
}
