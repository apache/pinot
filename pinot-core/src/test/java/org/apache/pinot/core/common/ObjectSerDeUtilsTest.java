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
package org.apache.pinot.core.common;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.tdunning.math.stats.TDigest;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.pinot.core.query.aggregation.function.PercentileEstAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.PercentileTDigestAggregationFunction;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.local.customobject.MinMaxRangePair;
import org.apache.pinot.segment.local.customobject.QuantileDigest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ObjectSerDeUtilsTest {
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final String ERROR_MESSAGE = "Random seed: " + RANDOM_SEED;

  private static final int NUM_ITERATIONS = 100;

  @Test
  public void testString() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      String expected = RandomStringUtils.random(RANDOM.nextInt(20));

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      String actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.String);

      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testLong() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      Long expected = RANDOM.nextLong();

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      Long actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.Long);

      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testDouble() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      Double expected = RANDOM.nextDouble();

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      Double actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.Double);

      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testDoubleArrayList() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      DoubleArrayList expected = new DoubleArrayList(size);
      for (int j = 0; j < size; j++) {
        expected.add(RANDOM.nextDouble());
      }

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      DoubleArrayList actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.DoubleArrayList);

      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testAvgPair() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      AvgPair expected = new AvgPair(RANDOM.nextDouble(), RANDOM.nextLong());

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      AvgPair actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.AvgPair);

      assertEquals(actual.getSum(), expected.getSum(), ERROR_MESSAGE);
      assertEquals(actual.getCount(), expected.getCount(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testMinMaxRangePair() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      MinMaxRangePair expected = new MinMaxRangePair(RANDOM.nextDouble(), RANDOM.nextDouble());

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      MinMaxRangePair actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.MinMaxRangePair);

      assertEquals(actual.getMin(), expected.getMin(), ERROR_MESSAGE);
      assertEquals(actual.getMax(), expected.getMax(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testHyperLogLog() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      HyperLogLog expected = new HyperLogLog(7);

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      HyperLogLog actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.HyperLogLog);

      assertEquals(actual.cardinality(), expected.cardinality(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testQuantileDigest() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      QuantileDigest expected = new QuantileDigest(PercentileEstAggregationFunction.DEFAULT_MAX_ERROR);
      int size = RANDOM.nextInt(100) + 1;
      for (int j = 0; j < size; j++) {
        expected.add(RANDOM.nextLong());
      }

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      QuantileDigest actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.QuantileDigest);

      for (int j = 0; j <= 100; j++) {
        assertEquals(actual.getQuantile(j / 100.0), expected.getQuantile(j / 100.0), 1e-5, ERROR_MESSAGE);
      }
    }
  }

  @Test
  public void testMap() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      Map<String, Double> expected = new HashMap<>(size);
      for (int j = 0; j < size; j++) {
        expected.put(RandomStringUtils.random(RANDOM.nextInt(20)), RANDOM.nextDouble());
      }

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      Map<String, Double> actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.Map);

      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testIntSet() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      IntSet expected = new IntOpenHashSet(size);
      for (int j = 0; j < size; j++) {
        expected.add(RANDOM.nextInt());
      }

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      IntSet actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.IntSet);

      // NOTE: use Object comparison instead of Collection comparison because the order might be different
      assertEquals((Object) actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testTDigest() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      TDigest expected = TDigest.createMergingDigest(PercentileTDigestAggregationFunction.DEFAULT_TDIGEST_COMPRESSION);
      int size = RANDOM.nextInt(100) + 1;
      for (int j = 0; j < size; j++) {
        expected.add(RANDOM.nextDouble());
      }

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      TDigest actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.TDigest);

      for (int j = 0; j <= 100; j++) {
        assertEquals(actual.quantile(j / 100.0), expected.quantile(j / 100.0), 1e-5);
      }
    }
  }
}
