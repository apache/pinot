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
import it.unimi.dsi.fastutil.doubles.Double2LongOpenHashMap;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.Float2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.pinot.core.query.aggregation.function.PercentileEstAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.PercentileTDigestAggregationFunction;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.local.customobject.DoubleLongPair;
import org.apache.pinot.segment.local.customobject.FloatLongPair;
import org.apache.pinot.segment.local.customobject.IntLongPair;
import org.apache.pinot.segment.local.customobject.LongLongPair;
import org.apache.pinot.segment.local.customobject.MinMaxRangePair;
import org.apache.pinot.segment.local.customobject.QuantileDigest;
import org.apache.pinot.segment.local.customobject.StringLongPair;
import org.apache.pinot.segment.local.customobject.ValueLongPair;
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
  public void testIntValueTimePair() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ValueLongPair<Integer> expected = new IntLongPair(RANDOM.nextInt(), RANDOM.nextLong());

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      ValueLongPair<Integer> actual = ObjectSerDeUtils.deserialize(bytes,
          ObjectSerDeUtils.ObjectType.IntLongPair);

      assertEquals(actual.getValue(), expected.getValue(), ERROR_MESSAGE);
      assertEquals(actual.getTime(), expected.getTime(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testLongValueTimePair() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ValueLongPair<Long> expected = new LongLongPair(RANDOM.nextLong(), RANDOM.nextLong());

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      ValueLongPair<Long> actual = ObjectSerDeUtils.deserialize(bytes,
          ObjectSerDeUtils.ObjectType.LongLongPair);

      assertEquals(actual.getValue(), expected.getValue(), ERROR_MESSAGE);
      assertEquals(actual.getTime(), expected.getTime(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testFloatValueTimePair() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ValueLongPair<Float> expected = new FloatLongPair(RANDOM.nextFloat(), RANDOM.nextLong());

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      ValueLongPair<Float> actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.FloatLongPair);

      assertEquals(actual.getValue(), expected.getValue(), ERROR_MESSAGE);
      assertEquals(actual.getTime(), expected.getTime(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testDoubleValueTimePair() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ValueLongPair<Double> expected = new DoubleLongPair(RANDOM.nextDouble(), RANDOM.nextLong());

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      ValueLongPair<Double> actual = ObjectSerDeUtils.deserialize(bytes,
          ObjectSerDeUtils.ObjectType.DoubleLongPair);

      assertEquals(actual.getValue(), expected.getValue(), ERROR_MESSAGE);
      assertEquals(actual.getTime(), expected.getTime(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testStringValueTimePair() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ValueLongPair<String> expected = new StringLongPair(String.valueOf(RANDOM.nextDouble()), RANDOM.nextLong());

      String temp = new String(expected.getValue().getBytes());
      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      ValueLongPair<String> actual = ObjectSerDeUtils.deserialize(bytes,
          ObjectSerDeUtils.ObjectType.StringLongPair);

      assertEquals(actual.getValue(), expected.getValue(), ERROR_MESSAGE);
      assertEquals(actual.getTime(), expected.getTime(), ERROR_MESSAGE);
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

  @Test
  public void testInt2LongMap() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      Int2LongOpenHashMap expected = new Int2LongOpenHashMap(size);
      for (int j = 0; j < size; j++) {
        expected.put(RANDOM.nextInt(20), RANDOM.nextLong());
      }

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      Int2LongOpenHashMap actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.Int2LongMap);

      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testLong2LongMap() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      Long2LongOpenHashMap expected = new Long2LongOpenHashMap(size);
      for (int j = 0; j < size; j++) {
        expected.put(RANDOM.nextLong(), RANDOM.nextLong());
      }

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      Long2LongOpenHashMap actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.Long2LongMap);

      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testFloat2LongMap() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      Float2LongOpenHashMap expected = new Float2LongOpenHashMap(size);
      for (int j = 0; j < size; j++) {
        expected.put(RANDOM.nextFloat(), RANDOM.nextLong());
      }

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      Float2LongOpenHashMap actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.Float2LongMap);

      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testDouble2LongMap() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      Double2LongOpenHashMap expected = new Double2LongOpenHashMap(size);
      for (int j = 0; j < size; j++) {
        expected.put(RANDOM.nextDouble(), RANDOM.nextLong());
      }

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      Double2LongOpenHashMap actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.Double2LongMap);

      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }
}
