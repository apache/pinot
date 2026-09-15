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
import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.TDigest;
import it.unimi.dsi.fastutil.doubles.Double2LongOpenHashMap;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.Float2LongOpenHashMap;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.theta.ThetaSetOperationBuilder;
import org.apache.datasketches.theta.ThetaSketch;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.datasketches.tuple.TupleSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummarySetOperations;
import org.apache.datasketches.tuple.aninteger.IntegerTupleSketch;
import org.apache.pinot.core.query.aggregation.function.PercentileEstAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.PercentileTDigestAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelStepEvent;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.apache.pinot.segment.local.customobject.CpcSketchAccumulator;
import org.apache.pinot.segment.local.customobject.DoubleLongPair;
import org.apache.pinot.segment.local.customobject.FloatLongPair;
import org.apache.pinot.segment.local.customobject.IntLongPair;
import org.apache.pinot.segment.local.customobject.LongLongPair;
import org.apache.pinot.segment.local.customobject.MinMaxRangePair;
import org.apache.pinot.segment.local.customobject.QuantileDigest;
import org.apache.pinot.segment.local.customobject.StringLongPair;
import org.apache.pinot.segment.local.customobject.ThetaSketchAccumulator;
import org.apache.pinot.segment.local.customobject.TupleIntSketchAccumulator;
import org.apache.pinot.segment.local.customobject.ValueLongPair;
import org.apache.pinot.segment.local.utils.UltraLogLogUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class ObjectSerDeUtilsTest {
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final String ERROR_MESSAGE = "Random seed: " + RANDOM_SEED;

  private static final int NUM_ITERATIONS = 100;
  private static final String TDIGEST_3_2_VERBOSE_COMPRESSION_20 =
      "AAAAAT+5mZmZmZmaP+zMzMzMzM1ANAAAAAAAAAAAABtAKgAAAAAAAD+5mZmZmZmaQBgAAAAAAAA/uZmZmZmZmkAcAAAAAAAAP7mZ"
          + "mZmZmZpAHAAAAAAAAD+5mZmZmZmaQBwAAAAAAAA/uZmZmZmZmkAmAAAAAAAAP7mZmZmZmZpAKgAAAAAAAD+5mZmZmZmaQCYAAAAA"
          + "AAA/uZmZmZmZmkAmAAAAAAAAP7mZmZmZmZpAJAAAAAAAAD+5mZmZmZmaQC4AAAAAAAA/uZmZmZmZmkAmAAAAAAAAP7mZmZmZmZpA"
          + "LAAAAAAAAD+5mZmZmZmaQCIAAAAAAAA/uZmZmZmZmkAyAAAAAAAAP8VVVVVVVVZAMQAAAAAAAD/gAAAAAAAAQCwAAAAAAAA/4AAA"
          + "AAAAAEAsAAAAAAAAP+AAAAAAAABAKAAAAAAAAD/gAAAAAAAAQCQAAAAAAAA/564UeuFHrkAUAAAAAAAAP+zMzMzMzM1AFAAAAAAA"
          + "AD/szMzMzMzNQBwAAAAAAAA/7MzMzMzMzUAQAAAAAAAAP+zMzMzMzM1ACAAAAAAAAD/szMzMzMzNP/AAAAAAAAA/7MzMzMzMzT/w"
          + "AAAAAAAAP+zMzMzMzM0=";
  private static final String TDIGEST_3_2_SMALL_COMPRESSION_1000 =
      "AAAAAj+INkk/eVmAQJLlmhJu+WhEegAAB9oTiABAP4AAADxBsko/gAAAPRQvNz+AAAA9fAMgP4AAAD20Elw/gAAAPexqgT+AAAA+"
          + "E5aAP4AAAD4yP88/gAAAPlJFjz+AAAA+c73OP4AAAD6LYDU/gAAAPp2zoz+AAAA+sOdBP4AAAD7FClg/gAAAPtotkD+AAAA+8GMX"
          + "P4AAAD8D32Y/gAAAPxArOT+AAAA/HSDzP4AAAD8qzbU/gAAAPzk/8j+AAAA/SIegP4AAAD9YtmU/gAAAP2nf1D+AAAA/fBmxP4AA"
          + "AD+Hvh4/gAAAP5IRRj+AAAA/nRWBP4AAAD+o294/gAAAP7V3mj+AAAA/wv55P4AAAD/RiSw/gAAAP+Ez2T+AAAA/8h6sP4AAAEAC"
          + "N00/gAAAQAwnIz+AAABAFveKP4AAAEAixUk/gAAAQC+yDj+AAABAPeWGP4AAAEBNjrY/gAAAQF7lsz+AAABAci3iP4AAAECD3G8/"
          + "gAAAQI/1LD+AAABAnZ6aP4AAAECtJUM/gAAAQL7pkj+AAABA02aDP4AAAEDrOzE/gAAAQQOcVz+AAABBFDtQP4AAAEEoOWA/gAAA"
          + "QUChLz+AAABBXvGHP4AAAEGCsOA/gAAAQZutuD+AAABBvS/yP4AAAEHr7EA/gAAAQhhGxT+AAABCTlsKP4AAAEKWasI/gAAAQvfH"
          + "yz+AAABDgpJvP4AAAESXLNE=";

  @Test
  public void testString() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      String expected = RandomStringUtils.secure().next(RANDOM.nextInt(20));

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
      ValueLongPair<Integer> actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.IntLongPair);

      assertEquals(actual.getValue(), expected.getValue(), ERROR_MESSAGE);
      assertEquals(actual.getTime(), expected.getTime(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testLongValueTimePair() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ValueLongPair<Long> expected = new LongLongPair(RANDOM.nextLong(), RANDOM.nextLong());

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      ValueLongPair<Long> actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.LongLongPair);

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
      ValueLongPair<Double> actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.DoubleLongPair);

      assertEquals(actual.getValue(), expected.getValue(), ERROR_MESSAGE);
      assertEquals(actual.getTime(), expected.getTime(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testStringValueTimePair() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      ValueLongPair<String> expected = new StringLongPair(RandomStringUtils.secure().next(10), RANDOM.nextLong());

      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      ValueLongPair<String> actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.StringLongPair);

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
  public void testHyperLogLogDeserializeThrowsForSizeMismatch()
      throws Exception {
    // We serialize a HLL w/ log2m of 12 and then trim 1024 bytes from the end of it and try to deserialize it. An
    // exception should occur because 2732 bytes are expected after the headers, but instead it will only find 1708.
    byte[] bytes = (new HyperLogLog(12)).getBytes();
    byte[] trimmed = Arrays.copyOfRange(bytes, 0, bytes.length - 1024);
    assertThrows(RuntimeException.class,
        () -> ObjectSerDeUtils.deserialize(trimmed, ObjectSerDeUtils.ObjectType.HyperLogLog));
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
        expected.put(RandomStringUtils.secure().next(RANDOM.nextInt(20)), RANDOM.nextDouble());
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

    // Try some custom compression values
    List<Double> compressionFactorsToTest = Arrays.asList(10d, 200d, 500d, 1000d, 10000d);
    for (double compressionFactor : compressionFactorsToTest) {
      for (int i = 0; i < NUM_ITERATIONS; i++) {
        TDigest expected = TDigest.createMergingDigest(compressionFactor);
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

  @DataProvider(name = "tdigest32Fixtures")
  public static Object[][] tdigest32Fixtures() {
    return new Object[][]{
        {TDIGEST_3_2_VERBOSE_COMPRESSION_20, 1, 20.0, 256L, 0.1, 0.9},
        {TDIGEST_3_2_SMALL_COMPRESSION_1000, 2, 1_000.0, 64L, 0.011822292565892623, 1209.4004609431959}
    };
  }

  @Test(dataProvider = "tdigest32Fixtures")
  public void testTDigest32Fixtures(String base64Bytes, int expectedEncoding, double expectedCompression,
      long expectedSize, double expectedMin, double expectedMax) {
    byte[] bytes = Base64.getDecoder().decode(base64Bytes);
    assertEquals(ByteBuffer.wrap(bytes).getInt(), expectedEncoding);
    TDigest digest = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.TDigest);
    assertEquals(digest.compression(), expectedCompression);
    assertEquals(digest.size(), expectedSize);
    assertEquals(digest.getMin(), expectedMin);
    assertEquals(digest.getMax(), expectedMax);

    long centroidWeight = 0L;
    double previousMean = Double.NEGATIVE_INFINITY;
    for (Centroid centroid : digest.centroids()) {
      assertTrue(Double.isFinite(centroid.mean()));
      assertTrue(centroid.count() > 0);
      assertTrue(centroid.mean() + 1e-12 >= previousMean);
      centroidWeight += centroid.count();
      previousMean = centroid.mean();
    }
    assertEquals(centroidWeight, expectedSize);

    double previousQuantile = Double.NEGATIVE_INFINITY;
    for (double quantile : new double[]{0.0, 0.5, 0.75, 0.95, 0.99, 1.0}) {
      double value = digest.quantile(quantile);
      assertTrue(Double.isFinite(value));
      assertTrue(value >= previousQuantile);
      previousQuantile = value;
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

  @Test
  public void testCpcSketch() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      CpcSketch sketch = new CpcSketch();
      int size = RANDOM.nextInt(100) + 1;
      for (int j = 0; j < size; j++) {
        sketch.update(RANDOM.nextLong());
      }

      byte[] bytes = ObjectSerDeUtils.serialize(sketch);
      CpcSketch actual =
          ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.CompressedProbabilisticCounting);

      assertEquals(actual.getEstimate(), sketch.getEstimate(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testIntArrayList() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      IntArrayList expected = new IntArrayList(size);
      for (int j = 0; j < size; j++) {
        expected.add(RANDOM.nextInt());
      }
      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      IntArrayList actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.IntArrayList);
      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testLongArrayList() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      LongArrayList expected = new LongArrayList(size);
      for (int j = 0; j < size; j++) {
        expected.add(RANDOM.nextLong());
      }
      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      LongArrayList actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.LongArrayList);
      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testFloatArrayList() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      FloatArrayList expected = new FloatArrayList(size);
      for (int j = 0; j < size; j++) {
        expected.add(RANDOM.nextFloat());
      }
      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      FloatArrayList actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.FloatArrayList);
      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testStringArrayList() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      ObjectArrayList<String> expected = new ObjectArrayList<>(size);
      for (int j = 0; j < size; j++) {
        expected.add(RandomStringUtils.secure().next(RANDOM.nextInt(20)));
      }
      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      ObjectArrayList<String> actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.StringArrayList);
      assertEquals(actual, expected, ERROR_MESSAGE);
    }
  }

  @Test
  public void testULL() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      UltraLogLog ull = UltraLogLog.create(12);
      int size = RANDOM.nextInt(100) + 1;
      for (int j = 0; j < size; j++) {
        UltraLogLogUtils.hashObject(RANDOM.nextLong()).ifPresent(ull::add);
      }

      byte[] bytes = ObjectSerDeUtils.serialize(ull);
      UltraLogLog actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.UltraLogLog);

      assertEquals(actual.getDistinctCountEstimate(), ull.getDistinctCountEstimate(), ERROR_MESSAGE);
      assertEquals(actual.getState(), ull.getState(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testThetaSketch() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      UpdatableThetaSketch input = UpdatableThetaSketch.builder().build();
      int size = RANDOM.nextInt(100) + 10;
      boolean shouldOrder = RANDOM.nextBoolean();

      for (int j = 0; j < size; j++) {
        input.update(j);
      }

      ThetaSketch sketch = input.compact(shouldOrder, null);

      byte[] bytes = ObjectSerDeUtils.serialize(sketch);
      ThetaSketch actual = ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.DataSketch);

      assertEquals(actual.getEstimate(), sketch.getEstimate(), ERROR_MESSAGE);
      assertEquals(actual.toByteArray(), sketch.toByteArray(), ERROR_MESSAGE);
      assertEquals(actual.isOrdered(), shouldOrder, ERROR_MESSAGE);
    }
  }

  @Test
  public void testThetaSketchAccumulator() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      UpdatableThetaSketch input = UpdatableThetaSketch.builder().build();
      int size = RANDOM.nextInt(100) + 10;

      for (int j = 0; j < size; j++) {
        input.update(j);
      }

      ThetaSetOperationBuilder setOperationBuilder = new ThetaSetOperationBuilder();
      ThetaSketchAccumulator accumulator = new ThetaSketchAccumulator(setOperationBuilder, 2);
      ThetaSketch sketch = input.compact(false, null);
      accumulator.apply(sketch);

      byte[] bytes = ObjectSerDeUtils.serialize(accumulator);
      ThetaSketchAccumulator actual =
          ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.ThetaSketchAccumulator);

      assertEquals(actual.getResult().getEstimate(), sketch.getEstimate(), ERROR_MESSAGE);
      assertEquals(actual.getResult().toByteArray(), sketch.toByteArray(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testTupleIntSketchAccumulator() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int lgK = 4;
      int size = RANDOM.nextInt(100) + 10;
      IntegerTupleSketch input = new IntegerTupleSketch(lgK, IntegerSummary.Mode.Sum);

      for (int j = 0; j < size; j++) {
        input.update(j, RANDOM.nextInt(100));
      }

      IntegerSummarySetOperations setOps =
          new IntegerSummarySetOperations(IntegerSummary.Mode.Sum, IntegerSummary.Mode.Sum);
      TupleIntSketchAccumulator accumulator = new TupleIntSketchAccumulator(setOps, (int) Math.pow(2, lgK), 2);
      TupleSketch<IntegerSummary> sketch = input.compact();
      accumulator.apply(sketch);

      byte[] bytes = ObjectSerDeUtils.serialize(accumulator);
      TupleIntSketchAccumulator actual =
          ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.TupleIntSketchAccumulator);

      assertEquals(actual.getResult().getEstimate(), sketch.getEstimate(), ERROR_MESSAGE);
      assertEquals(actual.getResult().toByteArray(), sketch.toByteArray(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testCpcSketchAccumulator() {
    int lgK = 4;
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100) + 10;
      CpcSketch sketch = new CpcSketch(lgK);

      for (int j = 0; j < size; j++) {
        sketch.update(j);
      }

      CpcSketchAccumulator accumulator = new CpcSketchAccumulator(lgK, 2);
      accumulator.apply(sketch);

      byte[] bytes = ObjectSerDeUtils.serialize(accumulator);
      CpcSketchAccumulator actual =
          ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.CpcSketchAccumulator);

      assertEquals(actual.getResult().getEstimate(), sketch.getEstimate(), ERROR_MESSAGE);
      assertEquals(actual.getResult().toByteArray(), sketch.toByteArray(), ERROR_MESSAGE);
    }
  }

  @Test
  public void testOrderedStringSet() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(100);
      ObjectLinkedOpenHashSet<String> expected = new ObjectLinkedOpenHashSet<>(size);
      for (int j = 0; j < size; j++) {
        expected.add(RandomStringUtils.secure().next(RANDOM.nextInt(20)));
      }
      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      ObjectLinkedOpenHashSet<String> actual =
          ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.OrderedStringSet);
      for (int j = 0; j < size; j++) {
        assertEquals(actual.get(j), expected.get(j), ERROR_MESSAGE);
      }
    }
  }

  @Test
  public void testFunnelStepEventAccumulator() {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      int size = RANDOM.nextInt(1000);
      PriorityQueue<FunnelStepEvent> expected = new PriorityQueue<>();
      for (int j = 0; j < size; j++) {
        expected.add(new FunnelStepEvent(RANDOM.nextLong(), RANDOM.nextInt()));
      }
      byte[] bytes = ObjectSerDeUtils.serialize(expected);
      PriorityQueue<FunnelStepEvent> actual =
          ObjectSerDeUtils.deserialize(bytes, ObjectSerDeUtils.ObjectType.FunnelStepEventAccumulator);
      while (!actual.isEmpty()) {
        assertEquals(actual.poll(), expected.poll(), ERROR_MESSAGE);
      }
    }
    // Test empty queue
    PriorityQueue<FunnelStepEvent> empty = new PriorityQueue<>();
    PriorityQueue<FunnelStepEvent> deserialized = ObjectSerDeUtils.deserialize(ObjectSerDeUtils.serialize(empty),
        ObjectSerDeUtils.ObjectType.FunnelStepEventAccumulator);
    assertTrue(deserialized.isEmpty());
  }
}
