/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.util;

import com.linkedin.pinot.common.utils.DataTableSerDe;
import com.linkedin.pinot.common.utils.DataTableSerDeRegistry;
import com.linkedin.pinot.common.utils.primitive.MutableLongValue;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction;
import com.linkedin.pinot.core.util.DataTableCustomSerDe;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for {@link DataTableCustomSerDe} class.
 */
public class DataTableCustomSerDeTest {
  private static final int NUM_ITERATIONS = 100;

  Random random;
  long randomSeed;
  DataTableSerDe serde;


  @BeforeClass
  private void setup() {
    randomSeed = System.currentTimeMillis();
    random = new Random(randomSeed);
    serde = new DataTableCustomSerDe();

    // Register the custom data table ser/de.
    DataTableSerDeRegistry.getInstance().register(new DataTableCustomSerDe());
  }

  /**
   * Test for ser/de of MutableLong
   */
  @Test
  public void testMutableLong() {

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      MutableLongValue expected = new MutableLongValue(random.nextLong());
      byte[] bytes = serde.serialize(expected);

      MutableLongValue actual = serde.deserialize(bytes, DataTableSerDe.DataType.MutableLong);
      Assert.assertEquals(actual.getValue(), expected.getValue(), "Random seed: " + random);
    }
  }

  /**
   * Test for ser/de of Double
   */
  @Test
  public void testDouble() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      Double expected = random.nextDouble();
      byte[] bytes = serde.serialize(expected);

      Double actual = serde.deserialize(bytes, DataTableSerDe.DataType.Double);
      Assert.assertEquals(actual, expected, "Random seed: " + randomSeed);
    }
  }

  /**
   * Test for ser/de of DoubleArrayList
   */
  @Test
  public void testDoubleArrayList() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = random.nextInt(100);
      DoubleArrayList expected = new DoubleArrayList(size);

      for (int j = 0; j < size; j++) {
        expected.add(random.nextDouble());
      }

      byte[] bytes = serde.serialize(expected);
      DoubleArrayList actual = serde.deserialize(bytes, DataTableSerDe.DataType.DoubleArrayList);

      for (int j = 0; j < size; j++) {
        Assert.assertEquals(actual.getDouble(j), expected.getDouble(j), "Random seed: " + randomSeed);
      }
    }
  }

  /**
   * Test for ser/de of AvgPair
   */
  @Test
  public void testAvgPair() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      AvgAggregationFunction.AvgPair expected =
          new AvgAggregationFunction.AvgPair(random.nextDouble(), random.nextLong());

      byte[] bytes = serde.serialize(expected);
      AvgAggregationFunction.AvgPair actual = serde.deserialize(bytes, DataTableSerDe.DataType.AvgPair);
      Assert.assertEquals(actual.getFirst(), expected.getFirst());
      Assert.assertEquals(actual.getSecond(), expected.getSecond(), "Random seed: " + randomSeed);
    }
  }

  /**
   * Test for ser/de of MinMaxRangePair
   */
  @Test
  public void testMinMaxRangePair() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      MinMaxRangeAggregationFunction.MinMaxRangePair expected =
          new MinMaxRangeAggregationFunction.MinMaxRangePair(random.nextDouble(), random.nextDouble());

      byte[] bytes = serde.serialize(expected);
      MinMaxRangeAggregationFunction.MinMaxRangePair actual =
          serde.deserialize(bytes, DataTableSerDe.DataType.MinMaxRangePair);
      Assert.assertEquals(actual.getFirst(), expected.getFirst());
      Assert.assertEquals(actual.getSecond(), expected.getSecond(), "Random seed: " + randomSeed);
    }
  }

  /**
   * Test for ser/de of HashMap&lt;String, Double&gt;
   */
  @Test
  public void testStringDoubleHashMap() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      Map<String, Double> expected = new HashMap<>();

      int size = random.nextInt(100);
      for (int j = 0; j < size; j++) {
        expected.put(RandomStringUtils.random(random.nextInt(20), true, true), random.nextDouble());
      }

      byte[] bytes = serde.serialize(expected);
      HashMap<String, Double> actual = serde.deserialize(bytes, DataTableSerDe.DataType.HashMap);

      Assert.assertEquals(actual.size(), expected.size(), "Random seed: " + randomSeed);
      for (Map.Entry<String, Double> entry : expected.entrySet()) {
        String key = entry.getKey();
        Assert.assertTrue(actual.containsKey(key), "Random seed: " + randomSeed);
        Assert.assertEquals(actual.get(key), entry.getValue(), "Random seed: " + randomSeed);
      }
    }
  }

  /**
   * Test for ser/de of IntOpenHashSet
   */
  @Test
  public void testIntOpenHashSet() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = random.nextInt(100);

      IntOpenHashSet expected = new IntOpenHashSet(size);
      for (int j = 0; j < size; j++) {
        expected.add(random.nextInt());
      }

      byte[] bytes = serde.serialize(expected);
      IntOpenHashSet actual = serde.deserialize(bytes, DataTableSerDe.DataType.IntOpenHashSet);

      Assert.assertEquals(actual.size(), expected.size(), "Random seed: " + randomSeed);
      IntIterator iterator = expected.iterator();
      while (iterator.hasNext()) {
        Assert.assertTrue(actual.contains(iterator.nextInt()), "Random seed: " + randomSeed);
      }
    }
  }
}
