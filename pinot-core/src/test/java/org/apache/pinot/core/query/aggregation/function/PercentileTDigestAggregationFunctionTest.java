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

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.function.IntPredicate;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction.SerializedIntermediateResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.utils.TDigestUtils;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class PercentileTDigestAggregationFunctionTest {
  private static final ExpressionContext EXPRESSION = ExpressionContext.forIdentifier("col");

  @DataProvider(name = "rowCounts")
  public static Object[][] rowCounts() {
    return new Object[][]{{0}, {1}, {255}, {256}, {257}, {10_003}};
  }

  @Test(dataProvider = "rowCounts")
  public void testRawNumericAggregation(int numRows) {
    SplittableRandom random = new SplittableRandom(42);
    double[] values = new double[numRows];
    for (int i = 0; i < numRows; i++) {
      values[i] = random.nextDouble();
    }

    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, false);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(numRows, resultHolder,
        Map.of(EXPRESSION, SyntheticBlockValSets.Double.create(null, values)));
    TDigest result = function.extractAggregationResult(resultHolder);

    Assert.assertTrue(result instanceof PercentileTDigestAccumulator);
    Assert.assertEquals(result.size(), numRows);
    Assert.assertEquals(result.compression(), PercentileTDigestAggregationFunction.DEFAULT_TDIGEST_COMPRESSION);
    if (numRows == 0) {
      Assert.assertTrue(Double.isNaN(function.extractFinalResult(result)));
    } else {
      double[] sortedValues = values.clone();
      Arrays.sort(sortedValues);
      double expected = sortedValues[(int) ((long) numRows * 75 / 100)];
      Assert.assertEquals(function.extractFinalResult(result), expected, 0.01);
    }

    TDigest deserialized = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(
        ObjectSerDeUtils.TDIGEST_SER_DE.serialize(result));
    Assert.assertTrue(deserialized instanceof MergingDigest);
    Assert.assertEquals(deserialized.size(), numRows);
    Assert.assertEquals(deserialized.quantile(0.75), result.quantile(0.75));
  }

  @Test
  public void testRawNumericAggregationWithNulls() {
    int numRows = 1_024;
    double[] values = new double[numRows];
    for (int i = 0; i < numRows; i++) {
      values[i] = i;
    }
    RoaringBitmap nullBitmap = RoaringBitmap.bitmapOf(0, 1, 254, 255, 256, 511, 512, 1_023);

    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, true);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(numRows, resultHolder,
        Map.of(EXPRESSION, SyntheticBlockValSets.Double.create(nullBitmap, values)));
    TDigest result = function.extractAggregationResult(resultHolder);

    Assert.assertEquals(result.size(), numRows - nullBitmap.getCardinality());
    double[] nonNullValues = new double[numRows - nullBitmap.getCardinality()];
    int index = 0;
    for (int i = 0; i < numRows; i++) {
      if (!nullBitmap.contains(i)) {
        nonNullValues[index++] = values[i];
      }
    }
    Assert.assertEquals(function.extractFinalResult(result), nonNullValues[nonNullValues.length * 75 / 100], 2.0);

    RoaringBitmap allNull = new RoaringBitmap();
    allNull.add(0L, numRows);
    resultHolder = function.createAggregationResultHolder();
    function.aggregate(numRows, resultHolder,
        Map.of(EXPRESSION, SyntheticBlockValSets.Double.create(allNull, values)));
    Assert.assertEquals(function.extractAggregationResult(resultHolder).size(), 0L);
  }

  @Test
  public void testRawMultiValueAggregation() {
    double[][] values = {{0.1, 0.2}, {}, {0.3}, {0.7, 0.8, 0.9}, {1.0}};
    RoaringBitmap nullBitmap = RoaringBitmap.bitmapOf(2);
    BlockValSet blockValSet = new SyntheticBlockValSets.Base() {
      @Override
      public RoaringBitmap getNullBitmap() {
        return nullBitmap;
      }

      @Override
      public DataType getValueType() {
        return DataType.DOUBLE;
      }

      @Override
      public boolean isSingleValue() {
        return false;
      }

      @Override
      public double[][] getDoubleValuesMV() {
        return values;
      }
    };
    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, true);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(values.length, resultHolder, Map.of(EXPRESSION, blockValSet));
    TDigest result = function.extractAggregationResult(resultHolder);

    Assert.assertEquals(result.size(), 6L);
    Assert.assertEquals(function.extractFinalResult(result), 0.8, 0.1);
  }

  @Test
  public void testDuplicateAndSpecialValues() {
    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 50.0, false);
    double[] duplicateValues = new double[10_003];
    Arrays.fill(duplicateValues, 42.0);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(duplicateValues.length, resultHolder,
        Map.of(EXPRESSION, SyntheticBlockValSets.Double.create(null, duplicateValues)));
    TDigest result = function.extractAggregationResult(resultHolder);
    Assert.assertEquals(result.size(), duplicateValues.length);
    Assert.assertEquals(function.extractFinalResult(result), 42.0);
    Assert.assertEquals(result.getMin(), 42.0);
    Assert.assertEquals(result.getMax(), 42.0);

    double[] specialValues = {Double.NEGATIVE_INFINITY, -1.0, -0.0, 0.0, 1.0, Double.POSITIVE_INFINITY};
    resultHolder = function.createAggregationResultHolder();
    function.aggregate(specialValues.length, resultHolder,
        Map.of(EXPRESSION, SyntheticBlockValSets.Double.create(null, specialValues)));
    result = function.extractAggregationResult(resultHolder);
    Assert.assertEquals(result.size(), specialValues.length);
    Assert.assertEquals(result.getMin(), Double.NEGATIVE_INFINITY);
    Assert.assertEquals(result.getMax(), Double.POSITIVE_INFINITY);
    Assert.assertEquals(result.quantile(0.0), Double.NEGATIVE_INFINITY);
    Assert.assertEquals(result.quantile(1.0), Double.POSITIVE_INFINITY);
    Assert.assertEquals(result.quantile(0.5), 0.0);
  }

  @Test
  public void testReverseMergeDoesNotTreatSignedZeroAsSameSign() {
    PercentileTDigestAccumulator accumulator = new PercentileTDigestAccumulator(10);
    accumulator.add(-0.0);
    accumulator.compress();
    accumulator.add(0.0);
    accumulator.add(Double.MIN_VALUE);
    accumulator.add(Double.MAX_VALUE, 9);
    accumulator.compress();

    List<Centroid> centroids = new ArrayList<>(accumulator.centroids());
    Assert.assertEquals(centroids.size(), 3);
    Assert.assertEquals(centroids.get(1).count(), 2);
    Assert.assertEquals(Double.doubleToRawLongBits(centroids.get(1).mean()),
        Double.doubleToRawLongBits(0.0));
  }

  @Test
  public void testDuplicateInfiniteValuesAcrossRawAndSerializedReduction() {
    int repetitions = 1_000;
    double[] values = new double[3 * repetitions];
    Arrays.fill(values, 0, repetitions, Double.NEGATIVE_INFINITY);
    Arrays.fill(values, repetitions, 2 * repetitions, 0.0);
    Arrays.fill(values, 2 * repetitions, values.length, Double.POSITIVE_INFINITY);

    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 50.0, 20, false);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(values.length, resultHolder,
        Map.of(EXPRESSION, SyntheticBlockValSets.Double.create(null, values)));
    TDigest rawResult = function.extractAggregationResult(resultHolder);
    assertDuplicateInfinityResult(rawResult, values.length);
    Assert.assertEquals(rawResult.cdf(-1.0), 1.0 / 3.0, 1e-12);
    Assert.assertEquals(rawResult.cdf(0.0), 0.5, 1e-12);
    Assert.assertEquals(rawResult.cdf(1.0), 2.0 / 3.0, 1e-12);

    byte[] serialized = function.serializeIntermediateResult(rawResult).getBytes();
    TDigest reducedResult = aggregateSerialized(new byte[][]{serialized, serialized}, null, false);
    assertDuplicateInfinityResult(reducedResult, 2L * values.length);
    Assert.assertEquals(reducedResult.cdf(-1.0), 1.0 / 3.0, 1e-12);
    Assert.assertEquals(reducedResult.cdf(0.0), 0.5, 1e-12);
    Assert.assertEquals(reducedResult.cdf(1.0), 2.0 / 3.0, 1e-12);
  }

  @Test
  public void testOnlyInfiniteValuesProduceMonotonicQuantiles() {
    int repetitions = 1_000;
    double[] values = new double[2 * repetitions];
    Arrays.fill(values, 0, repetitions, Double.NEGATIVE_INFINITY);
    Arrays.fill(values, repetitions, values.length, Double.POSITIVE_INFINITY);
    PercentileTDigestAccumulator accumulator = new PercentileTDigestAccumulator(20);
    accumulator.add(values, 0, values.length);

    double previous = Double.NEGATIVE_INFINITY;
    for (double quantile : new double[]{0.0, 0.25, 0.5, 0.75, 1.0}) {
      double value = accumulator.quantile(quantile);
      Assert.assertFalse(Double.isNaN(value));
      Assert.assertTrue(value >= previous);
      previous = value;
    }
    Assert.assertEquals(accumulator.cdf(0.0), 0.5, 1e-12);
  }

  @Test
  public void testLegacyWeightedBoundariesUseTheirSourceExtrema() {
    ByteBuffer legacy = ByteBuffer.allocate(64);
    legacy.putInt(1);
    legacy.putDouble(0.0);
    legacy.putDouble(10.0);
    legacy.putDouble(20.0);
    legacy.putInt(2);
    legacy.putDouble(2.0);
    legacy.putDouble(1.0);
    legacy.putDouble(1.0);
    legacy.putDouble(10.0);

    ByteBuffer shifted = ByteBuffer.allocate(64);
    shifted.putInt(1);
    shifted.putDouble(-100.0);
    shifted.putDouble(100.0);
    shifted.putDouble(20.0);
    shifted.putInt(2);
    shifted.putDouble(1.0);
    shifted.putDouble(-100.0);
    shifted.putDouble(1.0);
    shifted.putDouble(100.0);

    PercentileTDigestAccumulator actual = PercentileTDigestAccumulator.forReduction(20.0);
    actual.addSerializedTDigest(legacy.array());
    actual.addSerializedTDigest(shifted.array());
    PercentileTDigestAccumulator expected = new PercentileTDigestAccumulator(20);
    expected.add(new double[]{0.0, 2.0, 10.0, -100.0, 100.0}, 0, 5);

    Assert.assertEquals(actual.size(), 5L);
    for (double quantile : new double[]{0.0, 0.25, 0.5, 0.75, 1.0}) {
      Assert.assertEquals(actual.quantile(quantile), expected.quantile(quantile), 1e-12,
          "Legacy boundary repair used extrema from another source at p" + quantile * 100);
    }
  }

  @Test
  public void testSerializedAggregationVerboseAndSmallEncodings() {
    int numDigests = 32;
    int valuesPerDigest = 200;
    byte[][] verboseValues = createSerializedDigests(numDigests, valuesPerDigest, 100.0, ignored -> false);
    byte[][] smallValues = createSerializedDigests(numDigests, valuesPerDigest, 100.0, ignored -> true);

    TDigest verboseResult = aggregateSerialized(verboseValues, null, false);
    TDigest smallResult = aggregateSerialized(smallValues, null, false);
    long expectedSize = (long) numDigests * valuesPerDigest;
    double expectedPercentile = 0.75;
    for (TDigest result : new TDigest[]{verboseResult, smallResult}) {
      Assert.assertTrue(result instanceof PercentileTDigestAccumulator);
      Assert.assertEquals(result.size(), expectedSize);
      Assert.assertEquals(result.compression(), 100.0);
      Assert.assertEquals(result.quantile(0.75), expectedPercentile, 0.005);
      Assert.assertEquals(result.getMin(), 0.0);
      Assert.assertEquals(result.getMax(), (expectedSize - 1.0) / expectedSize, 0.000_001);
      Assert.assertTrue(((PercentileTDigestAccumulator) result).toTDigest() instanceof MergingDigest);
    }
    // The compact source narrows centroids to floats and can take a different valid merge path. Both results are
    // independently bounded against the exact percentile above, so keep their cross-encoding envelope equally tight.
    Assert.assertEquals(smallResult.quantile(0.75), verboseResult.quantile(0.75), 0.005);
  }

  @Test
  public void testSerializedAggregationWithFirstAndNonLeadingNulls() {
    int numDigests = 6;
    int valuesPerDigest = 20;
    byte[][] values = createSerializedDigests(numDigests, valuesPerDigest, 100.0, index -> (index & 1) == 0);
    RoaringBitmap nullBitmap = RoaringBitmap.bitmapOf(0, 2, 5);
    for (int index : nullBitmap) {
      values[index] = new byte[0];
    }

    TDigest result = aggregateSerialized(values, nullBitmap, true);
    int expectedSize = (numDigests - nullBitmap.getCardinality()) * valuesPerDigest;
    Assert.assertEquals(result.size(), expectedSize);
    double[] expectedValues = new double[expectedSize];
    int outputIndex = 0;
    for (int digestIndex = 0; digestIndex < numDigests; digestIndex++) {
      if (!nullBitmap.contains(digestIndex)) {
        for (int valueIndex = 0; valueIndex < valuesPerDigest; valueIndex++) {
          expectedValues[outputIndex++] = serializedValue(digestIndex, valueIndex, numDigests, valuesPerDigest);
        }
      }
    }
    Arrays.sort(expectedValues);
    Assert.assertEquals(result.getMin(), expectedValues[0]);
    Assert.assertEquals(result.getMax(), expectedValues[expectedSize - 1]);
    Assert.assertEquals(result.quantile(0.75), expectedValues[expectedSize * 75 / 100], 0.02);
  }

  @Test
  public void testSerializedAggregationAllNull() {
    byte[][] values = {new byte[0], new byte[0], new byte[0]};
    RoaringBitmap nullBitmap = new RoaringBitmap();
    nullBitmap.add(0L, values.length);

    TDigest result = aggregateSerialized(values, nullBitmap, true);
    Assert.assertEquals(result.size(), 0L);
    Assert.assertEquals(result.compression(), PercentileTDigestAggregationFunction.DEFAULT_TDIGEST_COMPRESSION);
    Assert.assertTrue(Double.isNaN(result.quantile(0.75)));
  }

  @Test
  public void testSerializedAggregationAcrossCallsAndBatchBoundaries() {
    int[] batchSizes = {1, 63, 64, 65, 257};
    int numDigests = Arrays.stream(batchSizes).sum();
    int valuesPerDigest = 10;
    byte[][] values = createSerializedDigests(numDigests, valuesPerDigest, 100.0, index -> (index & 1) != 0);
    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, false);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    int offset = 0;
    for (int batchSize : batchSizes) {
      byte[][] batch = Arrays.copyOfRange(values, offset, offset + batchSize);
      function.aggregate(batchSize, resultHolder, Map.of(EXPRESSION, bytesBlockValSet(batch, null)));
      offset += batchSize;
    }

    TDigest result = function.extractAggregationResult(resultHolder);
    long expectedSize = (long) numDigests * valuesPerDigest;
    Assert.assertEquals(result.size(), expectedSize);
    Assert.assertEquals(result.quantile(0.75), 0.75, 0.005);
  }

  @Test
  public void testSerializedAggregationPreservesFirstDigestDoubleCompression() {
    double compression = 42.125;
    byte[][] values = createSerializedDigests(8, 100, compression, ignored -> false);

    TDigest result = aggregateSerialized(values, null, false);
    Assert.assertEquals(result.size(), 800L);
    Assert.assertEquals(result.compression(), compression);
    Assert.assertEquals(result.quantile(0.75), 0.75, 0.01);
  }

  @Test
  public void testSerializedGroupBySV() {
    int numDigests = 12;
    int valuesPerDigest = 100;
    double compression = 42.125;
    byte[][] values = createSerializedDigests(numDigests, valuesPerDigest, compression, index -> (index & 1) == 0);
    int[] groupKeys = {0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2};
    RoaringBitmap nullBitmap = RoaringBitmap.bitmapOf(0, 7);
    for (int index : nullBitmap) {
      values[index] = new byte[0];
    }

    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, true);
    GroupByResultHolder resultHolder = function.createGroupByResultHolder(4, 4);
    function.aggregateGroupBySV(numDigests, groupKeys, resultHolder,
        Map.of(EXPRESSION, bytesBlockValSet(values, nullBitmap)));
    function.aggregateGroupBySV(numDigests, groupKeys, resultHolder,
        Map.of(EXPRESSION, bytesBlockValSet(values, nullBitmap)));

    assertSerializedAccumulatorState(resultHolder, 0);
    assertSerializedAccumulatorState(resultHolder, 1);
    assertSerializedAccumulatorState(resultHolder, 2);
    double denominator = numDigests * (double) valuesPerDigest;
    assertGroupResult(function, resultHolder, 0, 6L * valuesPerDigest, compression, 3.0 / denominator,
        (99.0 * numDigests + 9.0) / denominator);
    assertGroupResult(function, resultHolder, 1, 6L * valuesPerDigest, compression, 1.0 / denominator,
        (99.0 * numDigests + 10.0) / denominator);
    assertGroupResult(function, resultHolder, 2, 8L * valuesPerDigest, compression, 2.0 / denominator,
        (99.0 * numDigests + 11.0) / denominator);
    Assert.assertEquals(function.extractGroupByResult(resultHolder, 3).size(), 0L);
  }

  @Test
  public void testSerializedGroupByMV() {
    int numDigests = 6;
    int valuesPerDigest = 100;
    byte[][] values = createSerializedDigests(numDigests, valuesPerDigest, 100.0, index -> (index & 1) != 0);
    int[][] groupKeys = {{0, 1}, {1}, {0, 2}, {2, 3}, {0, 3}, {1, 2, 3}};
    RoaringBitmap nullBitmap = RoaringBitmap.bitmapOf(3);
    values[3] = new byte[0];

    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, true);
    GroupByResultHolder resultHolder = function.createGroupByResultHolder(4, 4);
    function.aggregateGroupByMV(numDigests, groupKeys, resultHolder,
        Map.of(EXPRESSION, bytesBlockValSet(values, nullBitmap)));

    for (int groupKey = 0; groupKey < 4; groupKey++) {
      assertSerializedAccumulatorState(resultHolder, groupKey);
    }
    double denominator = numDigests * (double) valuesPerDigest;
    assertGroupResult(function, resultHolder, 0, 3L * valuesPerDigest, 100.0, 0.0,
        (99.0 * numDigests + 4.0) / denominator);
    assertGroupResult(function, resultHolder, 1, 3L * valuesPerDigest, 100.0, 0.0,
        (99.0 * numDigests + 5.0) / denominator);
    assertGroupResult(function, resultHolder, 2, 2L * valuesPerDigest, 100.0, 2.0 / denominator,
        (99.0 * numDigests + 5.0) / denominator);
    assertGroupResult(function, resultHolder, 3, 2L * valuesPerDigest, 100.0, 4.0 / denominator,
        (99.0 * numDigests + 5.0) / denominator);
  }

  @Test
  public void testSerializedGroupByMVSharedInputEdgeCases()
      throws ReflectiveOperationException {
    TDigest empty = TDigest.createMergingDigest(20.0);
    TDigest one = TDigest.createMergingDigest(100.0);
    one.add(1.0);
    ByteBuffer smallBuffer = ByteBuffer.allocate(one.smallByteSize());
    one.asSmallBytes(smallBuffer);
    TDigest two = TDigest.createMergingDigest(100.0);
    two.add(2.0);
    byte[][] values = {
        new byte[0], ObjectSerDeUtils.TDIGEST_SER_DE.serialize(empty), smallBuffer.array(),
        ObjectSerDeUtils.TDIGEST_SER_DE.serialize(two)
    };
    int[][] groupKeys = {{}, {0, 1}, {0, 1, 2, 2}, {0, 1, 2, 3}};

    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, false);
    GroupByResultHolder resultHolder = function.createGroupByResultHolder(4, 4);
    function.aggregateGroupByMV(values.length, groupKeys, resultHolder,
        Map.of(EXPRESSION, bytesBlockValSet(values, null)));

    PercentileTDigestAccumulator pendingAccumulator = resultHolder.getResult(3);
    for (String fieldName
        : new String[]{"_rawValues", "_centroidMeans", "_outputMeans", "_incomingMeans"}) {
      Field field = PercentileTDigestAccumulator.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      Assert.assertNull(field.get(pendingAccumulator));
    }

    long[] expectedSizes = {2L, 2L, 3L, 1L};
    double[] expectedCompressions = {20.0, 20.0, 100.0, 100.0};
    double[] expectedMinimums = {1.0, 1.0, 1.0, 2.0};
    for (int groupKey = 0; groupKey < 4; groupKey++) {
      TDigest result = function.extractGroupByResult(resultHolder, groupKey);
      Assert.assertEquals(result.size(), expectedSizes[groupKey]);
      Assert.assertEquals(result.compression(), expectedCompressions[groupKey]);
      Assert.assertEquals(result.getMin(), expectedMinimums[groupKey]);
      Assert.assertEquals(result.getMax(), 2.0);
    }
  }

  @Test
  public void testSerializedGroupByMVPreservesOversizedSmallCapacity() {
    int numCentroids = 80;
    ByteBuffer oversizedSmall = ByteBuffer.allocate(Integer.BYTES + 2 * Double.BYTES + Float.BYTES
        + 3 * Short.BYTES + 2 * Float.BYTES * numCentroids);
    oversizedSmall.putInt(2);
    oversizedSmall.putDouble(0.0);
    oversizedSmall.putDouble(numCentroids - 1.0);
    oversizedSmall.putFloat(20.0F);
    oversizedSmall.putShort((short) numCentroids);
    oversizedSmall.putShort((short) 100);
    oversizedSmall.putShort((short) numCentroids);
    for (int i = 0; i < numCentroids; i++) {
      oversizedSmall.putFloat(1.0F);
      oversizedSmall.putFloat(i);
    }
    TDigest empty = TDigest.createMergingDigest(20.0);

    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, false);
    TDigest lazyResult = function.deserializeIntermediateResult(
        new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(), ByteBuffer.wrap(oversizedSmall.array())));
    assertOversizedSmallIntermediateRoundTrip(function, lazyResult, numCentroids, 2, numCentroids);

    GroupByResultHolder resultHolder = function.createGroupByResultHolder(1, 1);
    function.aggregateGroupByMV(2, new int[][]{{0}, {0}}, resultHolder,
        Map.of(EXPRESSION, bytesBlockValSet(
            new byte[][]{oversizedSmall.array(), ObjectSerDeUtils.TDIGEST_SER_DE.serialize(empty)}, null)));

    TDigest result = function.extractGroupByResult(resultHolder, 0);
    Assert.assertEquals(result.size(), numCentroids);
    Assert.assertEquals(result.compression(), 20.0);
    Assert.assertEquals(result.getMin(), 0.0);
    Assert.assertEquals(result.getMax(), numCentroids - 1.0);
    Assert.assertEquals(result.quantile(0.75), 59.5, 1.0);
    assertOversizedSmallIntermediateRoundTrip(function, result, numCentroids, 1, 70);
    result.add(numCentroids);
    Assert.assertEquals(result.size(), numCentroids + 1L);

    double preciseCompression = 20.0000001;
    TDigest preciseEmpty = TDigest.createMergingDigest(preciseCompression);
    GroupByResultHolder preciseResultHolder = function.createGroupByResultHolder(1, 1);
    function.aggregateGroupByMV(2, new int[][]{{0}, {0}}, preciseResultHolder,
        Map.of(EXPRESSION, bytesBlockValSet(
            new byte[][]{ObjectSerDeUtils.TDIGEST_SER_DE.serialize(preciseEmpty), oversizedSmall.array()}, null)));
    TDigest preciseResult = function.extractGroupByResult(preciseResultHolder, 0);
    Assert.assertEquals(preciseResult.compression(), preciseCompression);
    Assert.assertEquals(preciseResult.size(), numCentroids);
    preciseResult.compress();
    Assert.assertTrue(preciseResult.centroidCount() <= 2 * Math.ceil(preciseCompression) + 30);
  }

  private static void assertOversizedSmallIntermediateRoundTrip(PercentileTDigestAggregationFunction function,
      TDigest result, int expectedSize, int expectedEncoding, int maxCentroids) {
    SerializedIntermediateResult serializedResult = function.serializeIntermediateResult(result);
    Assert.assertEquals(serializedResult.getType(), ObjectSerDeUtils.ObjectType.TDigest.getValue());
    byte[] serializedBytes = serializedResult.getBytes();
    Assert.assertEquals(ByteBuffer.wrap(serializedBytes).getInt(), expectedEncoding);
    TDigest roundTripped = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(serializedBytes);
    Assert.assertEquals(roundTripped.size(), expectedSize);
    Assert.assertTrue(roundTripped.centroidCount() <= maxCentroids);
    Assert.assertEquals(roundTripped.compression(), 20.0);
    Assert.assertEquals(roundTripped.getMin(), 0.0);
    Assert.assertEquals(roundTripped.getMax(), expectedSize - 1.0);
    double previousValue = Double.NEGATIVE_INFINITY;
    for (double quantile : new double[]{0.0, 0.5, 0.75, 0.95, 0.99, 1.0}) {
      double value = roundTripped.quantile(quantile);
      Assert.assertTrue(Double.isFinite(value));
      Assert.assertTrue(value >= previousValue);
      Assert.assertEquals(value, result.quantile(quantile));
      previousValue = value;
    }
  }

  /// `quantile()` must reject `NaN` like the non-finite-aware digest does, instead of letting it slip past the
  /// range guard (every `NaN` comparison is false) and propagate through the computation.
  @Test
  public void testQuantileRejectsNaN() {
    PercentileTDigestAccumulator accumulator = PercentileTDigestAccumulator.forReduction(100.0);
    accumulator.add(1.0);
    IllegalArgumentException exception =
        Assert.expectThrows(IllegalArgumentException.class, () -> accumulator.quantile(Double.NaN));
    Assert.assertEquals(exception.getMessage(), "q should be in [0,1], got NaN");
  }

  /// `Centroid` stores the weight as an `int`, so `centroids()` must saturate like `MergingDigest.centroids()`
  /// rather than throwing on a centroid whose weight exceeds `Integer.MAX_VALUE`.
  @Test
  public void testCentroidsSaturateWeightExceedingIntegerMaxValue()
      throws ReflectiveOperationException {
    PercentileTDigestAccumulator accumulator = PercentileTDigestAccumulator.forReduction(100.0);
    accumulator.add(1.0);
    accumulator.compress();

    // A centroid weight above Integer.MAX_VALUE is only reachable from an externally produced digest, since
    // add(double, int) caps a single contribution at Integer.MAX_VALUE.
    double oversizedWeight = Integer.MAX_VALUE + 1_000.0;
    setAccumulatorField(accumulator, "_centroidWeights", new double[]{oversizedWeight});
    setAccumulatorField(accumulator, "_totalWeight", oversizedWeight);

    List<Centroid> centroids = new ArrayList<>(accumulator.centroids());
    Assert.assertEquals(centroids.size(), 1);
    Assert.assertEquals(centroids.get(0).mean(), 1.0);
    Assert.assertEquals(centroids.get(0).count(), Integer.MAX_VALUE);
  }

  @Test
  public void testSerializedGroupByMVSharedInputReservesBoundaryCapacity()
      throws ReflectiveOperationException {
    ByteBuffer oneCentroid = ByteBuffer.allocate(30 + 2 * Float.BYTES);
    oneCentroid.putInt(2);
    oneCentroid.putDouble(1.0);
    oneCentroid.putDouble(1.0);
    oneCentroid.putFloat(1_000_000.0F);
    oneCentroid.putShort((short) 1);
    oneCentroid.putShort((short) 1);
    oneCentroid.putShort((short) 1);
    oneCentroid.putFloat(1.0F);
    oneCentroid.putFloat(1.0F);
    TDigest empty = TDigest.createMergingDigest(20.0);
    byte[] emptyBytes = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(empty);
    PercentileTDigestAccumulator accumulator = PercentileTDigestAccumulator.forSerializedTDigest(emptyBytes);
    accumulator.addSerializedTDigest(emptyBytes);
    PercentileTDigestAccumulator.SerializedTDigestInput input =
        new PercentileTDigestAccumulator.SerializedTDigestInput();
    input.reset(oneCentroid.array());
    accumulator.addSerializedTDigest(input);

    Field meansField = PercentileTDigestAccumulator.SerializedTDigestInput.class.getDeclaredField("_means");
    meansField.setAccessible(true);
    Assert.assertEquals(((double[]) meansField.get(input)).length, 3);
    Assert.assertEquals(accumulator.toTDigest().size(), 1L);
  }

  @Test
  public void testSerializedAccumulatorAllocatesRawBufferLazily()
      throws ReflectiveOperationException {
    byte[] serializedDigest = createSerializedDigests(1, 100, 100.0, ignored -> false)[0];
    Field rawValuesField = PercentileTDigestAccumulator.class.getDeclaredField("_rawValues");
    rawValuesField.setAccessible(true);

    PercentileTDigestAccumulator accumulator = PercentileTDigestAccumulator.forSerializedTDigest(serializedDigest);
    Assert.assertNull(rawValuesField.get(accumulator));
    accumulator.add(new double[0], 0, 0);
    Assert.assertNull(rawValuesField.get(accumulator));
    accumulator.addSerializedTDigest(serializedDigest);
    accumulator.toTDigest();
    Assert.assertNull(rawValuesField.get(accumulator));

    accumulator.add(new double[]{1.0}, 0, 1);
    Assert.assertNotNull(rawValuesField.get(accumulator));
    accumulator.addSerializedTDigest(serializedDigest);
    TDigest result = accumulator.toTDigest();
    Assert.assertEquals(result.size(), 201L);
    Assert.assertEquals(result.getMin(), 0.0);
    Assert.assertEquals(result.getMax(), 1.0);
    Assert.assertNull(rawValuesField.get(
        PercentileTDigestAccumulator.forSerializedTDigestWithMergeBuffers(serializedDigest)));
    Assert.assertNotNull(rawValuesField.get(new PercentileTDigestAccumulator(100)));
  }

  @Test
  public void testDirectMergeAtConfiguredCompressionIsNotRepeated()
      throws ReflectiveOperationException {
    byte[][] serializedDigests = createSerializedDigests(2, 100, 100.0, ignored -> false);
    PercentileTDigestAccumulator accumulator =
        PercentileTDigestAccumulator.forSerializedTDigestWithMergeBuffers(serializedDigests[0]);
    PercentileTDigestAccumulator.SerializedTDigestInput input =
        new PercentileTDigestAccumulator.SerializedTDigestInput();
    for (byte[] serializedDigest : serializedDigests) {
      input.reset(serializedDigest);
      accumulator.addSerializedTDigestDirect(input);
    }

    int mergeCount = (int) getAccumulatorField(accumulator, "_mergeCount");
    Assert.assertTrue((boolean) getAccumulatorField(accumulator, "_publiclyCompressed"));
    accumulator.compress();
    Assert.assertEquals(getAccumulatorField(accumulator, "_mergeCount"), mergeCount);
    Assert.assertEquals(accumulator.size(), 200L);
  }

  @Test
  public void testSerializedAccumulatorGrowsBuffersForRawValues() {
    TDigest serializedDigest = TDigest.createMergingDigest(100.0);
    serializedDigest.add(-1.0);
    byte[] serializedBytes = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(serializedDigest);
    PercentileTDigestAccumulator accumulator =
        PercentileTDigestAccumulator.forSerializedTDigest(serializedBytes);
    accumulator.addSerializedTDigest(serializedBytes);

    double[] rawValues = new double[768];
    for (int i = 0; i < rawValues.length; i++) {
      rawValues[i] = i;
    }
    accumulator.add(rawValues, 0, rawValues.length);

    TDigest result = accumulator.toTDigest();
    Assert.assertEquals(result.size(), 769L);
    Assert.assertEquals(result.getMin(), -1.0);
    Assert.assertEquals(result.getMax(), 767.0);
  }

  @Test
  public void testSerializedAccumulatorPreservesCompressionAfterEmptyDigests() {
    TDigest first = TDigest.createMergingDigest(20.0);
    TDigest second = TDigest.createMergingDigest(30.0);
    TDigest third = TDigest.createMergingDigest(40.0);
    third.add(1.0);
    byte[] firstBytes = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(first);
    PercentileTDigestAccumulator accumulator = PercentileTDigestAccumulator.forSerializedTDigest(firstBytes);
    accumulator.addSerializedTDigest(firstBytes);
    accumulator.addSerializedTDigest(ObjectSerDeUtils.TDIGEST_SER_DE.serialize(second));
    accumulator.addSerializedTDigest(ObjectSerDeUtils.TDIGEST_SER_DE.serialize(third));

    TDigest result = accumulator.toTDigest();
    Assert.assertEquals(result.compression(), 20.0);
    Assert.assertEquals(result.size(), 1L);
    Assert.assertEquals(result.quantile(0.5), 1.0);
  }

  @Test
  public void testRawNumericGroupByStillUsesTDigestState() {
    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, false);
    GroupByResultHolder resultHolder = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupBySV(4, new int[]{0, 0, 1, 1}, resultHolder,
        Map.of(EXPRESSION, SyntheticBlockValSets.Double.create(null, new double[]{1.0, 2.0, 3.0, 4.0})));

    Assert.assertTrue(resultHolder.getResult(0) instanceof TDigest);
    Assert.assertEquals(function.extractGroupByResult(resultHolder, 0).size(), 2L);
    Assert.assertEquals(function.extractGroupByResult(resultHolder, 1).size(), 2L);
  }

  @Test
  public void testDuplicateInfiniteValuesInRawGroupBy() {
    int repetitions = 1_000;
    double[] values = new double[3 * repetitions];
    Arrays.fill(values, 0, repetitions, Double.NEGATIVE_INFINITY);
    Arrays.fill(values, repetitions, 2 * repetitions, 0.0);
    Arrays.fill(values, 2 * repetitions, values.length, Double.POSITIVE_INFINITY);

    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 50.0, 20, false);
    GroupByResultHolder resultHolder = function.createGroupByResultHolder(1, 1);
    function.aggregateGroupBySV(values.length, new int[values.length], resultHolder,
        Map.of(EXPRESSION, SyntheticBlockValSets.Double.create(null, values)));

    TDigest result = function.extractGroupByResult(resultHolder, 0);
    assertDuplicateInfinityResult(result, values.length);
    TDigest roundTripped = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(
        function.serializeIntermediateResult(result).getBytes());
    assertDuplicateInfinityResult(roundTripped, values.length);
  }

  @DataProvider(name = "malformedSerializedDigests")
  public static Object[][] malformedSerializedDigests() {
    ByteBuffer invalidEncoding = ByteBuffer.allocate(Integer.BYTES);
    invalidEncoding.putInt(99);
    ByteBuffer truncatedVerbose = ByteBuffer.allocate(4 * Integer.BYTES + 2 * Double.BYTES);
    truncatedVerbose.putInt(1);
    truncatedVerbose.putDouble(0.0);
    truncatedVerbose.putDouble(1.0);
    truncatedVerbose.putDouble(100.0);
    truncatedVerbose.putInt(1);
    ByteBuffer truncatedSmall = ByteBuffer.allocate(3 * Integer.BYTES + 2 * Double.BYTES + Short.BYTES);
    truncatedSmall.putInt(2);
    truncatedSmall.putDouble(0.0);
    truncatedSmall.putDouble(1.0);
    truncatedSmall.putFloat(100.0F);
    truncatedSmall.putShort((short) 210);
    truncatedSmall.putShort((short) 500);
    truncatedSmall.putShort((short) 1);
    ByteBuffer oversizedVerbose = ByteBuffer.allocate(4 * Integer.BYTES + 2 * Double.BYTES);
    oversizedVerbose.putInt(1);
    oversizedVerbose.putDouble(0.0);
    oversizedVerbose.putDouble(1.0);
    oversizedVerbose.putDouble(100.0);
    oversizedVerbose.putInt(10_000_000);
    ByteBuffer oversizedSmall = ByteBuffer.allocate(3 * Integer.BYTES + 2 * Double.BYTES + Short.BYTES);
    oversizedSmall.putInt(2);
    oversizedSmall.putDouble(0.0);
    oversizedSmall.putDouble(1.0);
    oversizedSmall.putFloat(100.0F);
    oversizedSmall.putShort((short) 210);
    oversizedSmall.putShort((short) 500);
    oversizedSmall.putShort(Short.MAX_VALUE);
    ByteBuffer negativeMainCapacity = ByteBuffer.allocate(30);
    negativeMainCapacity.putInt(2);
    negativeMainCapacity.putDouble(0.0);
    negativeMainCapacity.putDouble(1.0);
    negativeMainCapacity.putFloat(100.0F);
    negativeMainCapacity.putShort((short) -2);
    negativeMainCapacity.putShort((short) 1);
    negativeMainCapacity.putShort((short) 0);
    ByteBuffer negativeBufferCapacity = ByteBuffer.allocate(30);
    negativeBufferCapacity.putInt(2);
    negativeBufferCapacity.putDouble(0.0);
    negativeBufferCapacity.putDouble(1.0);
    negativeBufferCapacity.putFloat(100.0F);
    negativeBufferCapacity.putShort((short) 1);
    negativeBufferCapacity.putShort((short) -2);
    negativeBufferCapacity.putShort((short) 0);
    ByteBuffer centroidsExceedMainCapacity = ByteBuffer.allocate(30 + 4 * Float.BYTES);
    centroidsExceedMainCapacity.putInt(2);
    centroidsExceedMainCapacity.putDouble(0.0);
    centroidsExceedMainCapacity.putDouble(1.0);
    centroidsExceedMainCapacity.putFloat(100.0F);
    centroidsExceedMainCapacity.putShort((short) 1);
    centroidsExceedMainCapacity.putShort((short) 2);
    centroidsExceedMainCapacity.putShort((short) 2);
    centroidsExceedMainCapacity.putFloat(1.0F);
    centroidsExceedMainCapacity.putFloat(0.0F);
    centroidsExceedMainCapacity.putFloat(1.0F);
    centroidsExceedMainCapacity.putFloat(1.0F);
    return new Object[][]{
        {invalidEncoding.array()}, {truncatedVerbose.array()}, {truncatedSmall.array()}, {oversizedVerbose.array()},
        {oversizedSmall.array()}, {negativeMainCapacity.array()}, {negativeBufferCapacity.array()},
        {centroidsExceedMainCapacity.array()}
    };
  }

  @Test
  public void testLowCompressionVerboseCapacityAddedIn33() {
    int numCentroids = 51;
    byte[] verbose = createVerboseUnitCentroidDigest(numCentroids, 20.0);

    TDigest deserialized = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(verbose);
    Assert.assertEquals(deserialized.size(), numCentroids);
    Assert.assertEquals(deserialized.centroidCount(), numCentroids);
    Assert.assertEquals(deserialized.getMin(), 0.0);
    Assert.assertEquals(deserialized.getMax(), numCentroids - 1.0);
  }

  @Test
  public void testFractionalCompressionVerboseCapacityAcrossVersions() {
    for (Object[] testCase : new Object[][]{{42.125, 95}, {100.1, 212}}) {
      double compression = (double) testCase[0];
      int numCentroids = (int) testCase[1];
      byte[] verbose = createVerboseUnitCentroidDigest(numCentroids, compression);

      TDigest result = aggregateSerialized(new byte[][]{verbose}, null, false);
      Assert.assertEquals(result.size(), numCentroids);
      Assert.assertEquals(result.compression(), compression);
      Assert.assertEquals(result.getMin(), 0.0);
      Assert.assertEquals(result.getMax(), numCentroids - 1.0);

      byte[] serialized = ((PercentileTDigestAccumulator) result).serialize();
      TDigest roundTripped = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(serialized);
      Assert.assertEquals(roundTripped.size(), numCentroids);
      Assert.assertEquals(roundTripped.compression(), compression);
      Assert.assertEquals(roundTripped.getMin(), 0.0);
      Assert.assertEquals(roundTripped.getMax(), numCentroids - 1.0);
    }
  }

  @Test
  public void testPendingLowCompressionDigestSerializesWithLegacyCompatibleCapacity() {
    int numCentroids = 51;
    byte[] verbose = createVerboseUnitCentroidDigest(numCentroids, 20.0);
    PercentileTDigestAccumulator accumulator = PercentileTDigestAccumulator.forSerializedTDigest(verbose);
    accumulator.addSerializedTDigest(verbose);

    ByteBuffer serialized = ByteBuffer.wrap(accumulator.serialize());
    Assert.assertEquals(serialized.getInt(), 2);
    serialized.position(Integer.BYTES + 2 * Double.BYTES + Float.BYTES);
    int mainCapacity = serialized.getShort();
    serialized.getShort();
    Assert.assertTrue(mainCapacity >= numCentroids);
    Assert.assertEquals(serialized.getShort(), numCentroids);

    TDigest roundTripped = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(serialized.array());
    Assert.assertEquals(roundTripped.size(), numCentroids);
    Assert.assertEquals(roundTripped.getMin(), 0.0);
    Assert.assertEquals(roundTripped.getMax(), numCentroids - 1.0);
  }

  @Test
  public void testPendingSerializedDigestOwnsInputBytes() {
    assertPendingSerializedDigestOwnsInputBytes(false);
    assertPendingSerializedDigestOwnsInputBytes(true);
  }

  @Test
  public void testAccumulatorSerializationDoesNotNarrowLargeMeans()
      throws ReflectiveOperationException {
    int centroidCount = 51;
    double min = 1.0e18;
    PercentileTDigestAccumulator accumulator = new PercentileTDigestAccumulator(20);
    double[] means = new double[70];
    double[] weights = new double[70];
    for (int i = 0; i < centroidCount; i++) {
      means[i] = min + 256.0 * i;
      weights[i] = 1.0;
    }
    setAccumulatorField(accumulator, "_centroidMeans", means);
    setAccumulatorField(accumulator, "_centroidWeights", weights);
    setAccumulatorField(accumulator, "_numCentroids", centroidCount);
    setAccumulatorField(accumulator, "_totalWeight", (double) centroidCount);
    setAccumulatorField(accumulator, "_min", means[0]);
    setAccumulatorField(accumulator, "_max", means[centroidCount - 1]);
    setAccumulatorField(accumulator, "_publiclyCompressed", true);

    byte[] serialized = accumulator.serialize();
    ByteBuffer header = ByteBuffer.wrap(serialized);
    Assert.assertEquals(header.getInt(), 1);
    header.position(Integer.BYTES + 3 * Double.BYTES);
    Assert.assertEquals(header.getInt(), 50);
    TDigest roundTripped = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(serialized);
    Assert.assertEquals(roundTripped.size(), centroidCount);
    Assert.assertEquals(roundTripped.getMin(), means[0]);
    Assert.assertEquals(roundTripped.getMax(), means[centroidCount - 1]);
    Assert.assertTrue(Double.isFinite(roundTripped.quantile(0.5)));
  }

  @Test
  public void testCentroidInspectionPreservesWeightedHighMagnitudeMean() {
    PercentileTDigestAccumulator accumulator = new PercentileTDigestAccumulator(100);
    accumulator.add(1.0e308, 2);

    long weight = 0L;
    for (Centroid centroid : accumulator.centroids()) {
      Assert.assertEquals(centroid.mean(), 1.0e308);
      weight += centroid.count();
    }
    Assert.assertEquals(weight, 2L);
  }

  @Test
  public void testSerializedBoundaryRepairDoesNotOverflowFirstMoment() {
    double min = 8.0e307;
    double mean = 1.0e308;
    double max = 1.2e308;
    ByteBuffer verbose = ByteBuffer.allocate(32 + 2 * Double.BYTES);
    verbose.putInt(1);
    verbose.putDouble(min);
    verbose.putDouble(max);
    verbose.putDouble(100.0);
    verbose.putInt(1);
    verbose.putDouble(3.0);
    verbose.putDouble(mean);

    TDigest result = aggregateSerialized(new byte[][]{verbose.array()}, null, false);
    List<Centroid> centroids = List.copyOf(result.centroids());
    Assert.assertEquals(centroids.size(), 3);
    Assert.assertEquals(centroids.get(0).mean(), min);
    Assert.assertEquals(centroids.get(1).mean(), mean, 4.0 * Math.ulp(mean));
    Assert.assertEquals(centroids.get(2).mean(), max);
  }

  @Test(dataProvider = "malformedSerializedDigests")
  public void testMalformedSerializedDigestMatchesMergingDigest(byte[] bytes) {
    RuntimeException expected = Assert.expectThrows(RuntimeException.class,
        () -> ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(bytes));
    RuntimeException actual = Assert.expectThrows(RuntimeException.class,
        () -> aggregateSerialized(new byte[][]{bytes}, null, false));
    Assert.assertEquals(actual.getClass(), expected.getClass());
    Assert.assertEquals(actual.getMessage(), expected.getMessage());

    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, false);
    GroupByResultHolder resultHolder = function.createGroupByResultHolder(1, 1);
    RuntimeException groupByActual = Assert.expectThrows(RuntimeException.class,
        () -> function.aggregateGroupBySV(1, new int[]{0}, resultHolder,
            Map.of(EXPRESSION, bytesBlockValSet(new byte[][]{bytes}, null))));
    Assert.assertEquals(groupByActual.getClass(), expected.getClass());
    Assert.assertEquals(groupByActual.getMessage(), expected.getMessage());

    TDigest empty = TDigest.createMergingDigest(100.0);
    byte[] emptyBytes = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(empty);
    RuntimeException materializedActual = Assert.expectThrows(RuntimeException.class,
        () -> aggregateSerialized(new byte[][]{emptyBytes, bytes}, null, false));
    Assert.assertEquals(materializedActual.getClass(), expected.getClass());
    Assert.assertEquals(materializedActual.getMessage(), expected.getMessage());

    GroupByResultHolder groupByMVResultHolder = function.createGroupByResultHolder(1, 1);
    RuntimeException groupByMVActual = Assert.expectThrows(RuntimeException.class,
        () -> function.aggregateGroupByMV(2, new int[][]{{0}, {0}}, groupByMVResultHolder,
            Map.of(EXPRESSION, bytesBlockValSet(new byte[][]{emptyBytes, bytes}, null))));
    Assert.assertEquals(groupByMVActual.getClass(), expected.getClass());
    Assert.assertEquals(groupByMVActual.getMessage(), expected.getMessage());
  }

  @DataProvider(name = "compressionFactors")
  public static Object[][] compressionFactors() {
    return new Object[][]{{10}, {50}, {100}, {200}, {1_000}, {10_000}};
  }

  @DataProvider(name = "reducerMergeCases")
  public static Object[][] reducerMergeCases() {
    List<Object[]> cases = new ArrayList<>();
    for (int fanIn : new int[]{8, 32, 128}) {
      for (int compression : new int[]{20, 100, 1_000}) {
        for (ReducerDistribution distribution : ReducerDistribution.values()) {
          for (MergeOrder mergeOrder : MergeOrder.values()) {
            for (ReducerInput reducerInput : ReducerInput.values()) {
              cases.add(new Object[]{fanIn, compression, distribution, mergeOrder, reducerInput});
            }
          }
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "reducerMergeCases")
  public void testReducerMergeStructureAndAccuracy(int fanIn, int compression, ReducerDistribution distribution,
      MergeOrder mergeOrder, ReducerInput reducerInput) {
    int valuesPerDigest = 64;
    int numValues = fanIn * valuesPerDigest;
    double[] rawValues = new double[numValues];
    TDigest[] sources = new TDigest[fanIn];
    TDigest[] referenceSources = new TDigest[fanIn];
    for (int digestIndex = 0; digestIndex < fanIn; digestIndex++) {
      SplittableRandom random = new SplittableRandom(0x5EEDL + digestIndex);
      double[] sourceValues = new double[valuesPerDigest];
      for (int valueIndex = 0; valueIndex < valuesPerDigest; valueIndex++) {
        double value = distribution.value(random.nextDouble());
        rawValues[digestIndex * valuesPerDigest + valueIndex] = value;
        sourceValues[valueIndex] = value;
      }
      sources[digestIndex] = createReducerSource(sourceValues, compression, reducerInput);
      referenceSources[digestIndex] = createReferenceReducerSource(sourceValues, compression, reducerInput);
    }

    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, compression, false);
    TDigest result = TDigest.createMergingDigest(compression);
    TDigest referenceResult = TDigest.createMergingDigest(compression);
    AssertionError referenceFailure = null;
    for (int sourceIndex : mergeOrder.sourceIndexes(fanIn)) {
      result = function.merge(result, sources[sourceIndex]);
      if (referenceFailure == null) {
        try {
          if (referenceResult.size() == 0L) {
            referenceResult = referenceSources[sourceIndex];
          } else {
            referenceResult.add(referenceSources[sourceIndex]);
          }
        } catch (AssertionError e) {
          // t-digest 3.3 can violate its own singleton-endpoint assertion when many duplicate-valued digests merge.
          referenceFailure = e;
        }
      }
    }

    Arrays.sort(rawValues);
    String caseDescription =
        fanIn + "/" + compression + "/" + distribution + "/" + mergeOrder + "/" + reducerInput;
    // Interpolation between large equal-valued centroid runs can land inside the empirical CDF jump. Keep a fixed
    // envelope for that discontinuous distribution instead of requiring every merge order to match the reference's
    // particular centroid partition.
    double frozenRankError = compression == 20 || distribution == ReducerDistribution.DUPLICATE_HEAVY ? 0.06 : 0.02;
    double[] referenceRankErrors = null;
    if (referenceFailure == null) {
      try {
        referenceRankErrors = getReducerRankErrors(referenceResult, rawValues, caseDescription + "/reference");
      } catch (AssertionError e) {
        referenceFailure = e;
      }
    }
    double[] maxRankErrors = new double[6];
    if (referenceFailure == null) {
      for (int i = 0; i < referenceRankErrors.length; i++) {
        maxRankErrors[i] = Math.max(frozenRankError, referenceRankErrors[i] + 1e-12);
      }
    } else {
      Arrays.fill(maxRankErrors, compression == 20 ? 0.25 : compression == 100 ? 0.06 : 0.02);
    }
    assertValidReducerResult(result, rawValues, caseDescription, maxRankErrors);

    SerializedIntermediateResult serializedResult = function.serializeIntermediateResult(result);
    Assert.assertEquals(serializedResult.getType(), ObjectSerDeUtils.ObjectType.TDigest.getValue());
    TDigest roundTripped = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(serializedResult.getBytes());
    Assert.assertTrue(roundTripped instanceof MergingDigest);
    for (double quantile : new double[]{0.0, 0.5, 0.75, 0.95, 0.99, 1.0}) {
      Assert.assertEquals(result.quantile(quantile), roundTripped.quantile(quantile), 1e-15,
          "Wire round trip changed p" + quantile * 100 + " for " + caseDescription);
    }
    getReducerRankErrors(roundTripped, rawValues, caseDescription + "/standard-wire");
  }

  @Test
  public void testReducerMergeEmptyAndSingleton() {
    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, false);
    for (ReducerInput reducerInput : ReducerInput.values()) {
      TDigest empty = createReducerSource(new double[0], 100, reducerInput);
      TDigest singleton = createReducerSource(new double[]{42.0}, 100, reducerInput);

      TDigest result = function.merge(empty, singleton);
      Assert.assertSame(result, singleton);
      Assert.assertSame(function.merge(result, createReducerSource(new double[0], 100, reducerInput)), result);
      assertValidReducerResult(result, new double[]{42.0}, "singleton/" + reducerInput);
    }
  }

  @Test(dataProvider = "compressionFactors")
  public void testCustomCompression(int compression) {
    int numRows = 20_003;
    SplittableRandom random = new SplittableRandom(42);
    double[] values = new double[numRows];
    for (int i = 0; i < numRows; i++) {
      values[i] = random.nextDouble();
    }
    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, compression, false);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(numRows, resultHolder,
        Map.of(EXPRESSION, SyntheticBlockValSets.Double.create(null, values)));
    TDigest result = function.extractAggregationResult(resultHolder);

    Assert.assertEquals(result.size(), numRows);
    Assert.assertEquals(result.compression(), compression);
    double[] sortedValues = values.clone();
    Arrays.sort(sortedValues);
    double actual = function.extractFinalResult(result);
    double maxAbsoluteError = compression == 10 ? 0.03 : 0.02;
    Assert.assertEquals(actual, sortedValues[numRows * 75 / 100], maxAbsoluteError);

    TDigest reference = TDigestUtils.createMergingDigest(compression);
    for (double value : values) {
      reference.add(value);
    }
    Assert.assertEquals(actual, reference.quantile(0.75), 0.001,
        "Pinot accumulator diverges from t-digest 3.3 K1 at compression " + compression);
  }

  @Test
  public void testRawAndDirectLowCompressionHeavyTailAccuracy() {
    int numDigests = 32;
    int valuesPerDigest = 512;
    int compression = 20;
    int numValues = numDigests * valuesPerDigest;
    double[] values = new double[numValues];
    byte[][] serializedDigests = new byte[numDigests][];
    for (int digestIndex = 0; digestIndex < numDigests; digestIndex++) {
      TDigest digest = TDigestUtils.createMergingDigest(compression);
      SplittableRandom random = new SplittableRandom(0x5EEDL + digestIndex);
      for (int valueIndex = 0; valueIndex < valuesPerDigest; valueIndex++) {
        double value = ReducerDistribution.HEAVY_TAIL.value(random.nextDouble());
        values[digestIndex * valuesPerDigest + valueIndex] = value;
        digest.add(value);
      }
      serializedDigests[digestIndex] = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(digest);
    }

    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, compression, false);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(values.length, resultHolder,
        Map.of(EXPRESSION, SyntheticBlockValSets.Double.create(null, values)));
    TDigest rawResult = function.extractAggregationResult(resultHolder);
    TDigest directResult = aggregateSerialized(serializedDigests, null, false);

    Arrays.sort(values);
    assertValidReducerResult(rawResult, values, "raw/20/heavy-tail", 0.06);
    assertValidReducerResult(directResult, values, "direct/20/heavy-tail", 0.06);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = "Cannot add NaN to t-digest")
  public void testNaNRejected() {
    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, false);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(3, resultHolder,
        Map.of(EXPRESSION, SyntheticBlockValSets.Double.create(null, new double[]{1.0, Double.NaN, 2.0})));
  }

  @Test
  public void testCanUseStarTreeDefaultCompression() {
    PercentileTDigestAggregationFunction function = new PercentileTDigestAggregationFunction(
        EXPRESSION, 95, false);

    Assert.assertTrue(function.canUseStarTree(Map.of()));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, "100")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, 100)));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, "200")));

    function = new PercentileTDigestAggregationFunction(EXPRESSION, 99, 100, true);
    Assert.assertTrue(function.canUseStarTree(Map.of()));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, "100")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, 100)));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, 200)));
  }

  @Test
  public void testCanUseStarTreeCustomCompression() {
    PercentileTDigestAggregationFunction function = new PercentileTDigestAggregationFunction(
        EXPRESSION, 95, 200, false);

    Assert.assertFalse(function.canUseStarTree(Map.of()));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, "100")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, "200")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, 200)));
  }

  private static byte[][] createSerializedDigests(int numDigests, int valuesPerDigest, double compression,
      IntPredicate useSmallEncoding) {
    byte[][] serializedDigests = new byte[numDigests][];
    for (int digestIndex = 0; digestIndex < numDigests; digestIndex++) {
      TDigest digest = TDigestUtils.createMergingDigest(compression);
      for (int valueIndex = 0; valueIndex < valuesPerDigest; valueIndex++) {
        digest.add(serializedValue(digestIndex, valueIndex, numDigests, valuesPerDigest));
      }
      if (useSmallEncoding.test(digestIndex)) {
        ByteBuffer buffer = ByteBuffer.allocate(digest.smallByteSize());
        digest.asSmallBytes(buffer);
        serializedDigests[digestIndex] = buffer.array();
      } else {
        serializedDigests[digestIndex] = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(digest);
      }
    }
    return serializedDigests;
  }

  private static byte[] createVerboseUnitCentroidDigest(int numCentroids, double compression) {
    ByteBuffer verbose = ByteBuffer.allocate(4 * Integer.BYTES + 2 * Double.BYTES
        + 2 * Double.BYTES * numCentroids);
    verbose.putInt(1);
    verbose.putDouble(0.0);
    verbose.putDouble(numCentroids - 1.0);
    verbose.putDouble(compression);
    verbose.putInt(numCentroids);
    for (int i = 0; i < numCentroids; i++) {
      verbose.putDouble(1.0);
      verbose.putDouble(i);
    }
    return verbose.array();
  }

  private static void assertPendingSerializedDigestOwnsInputBytes(boolean reusableInput) {
    byte[] bytes = createVerboseUnitCentroidDigest(3, 20.0);
    byte[] expected = bytes.clone();
    PercentileTDigestAccumulator accumulator;
    if (reusableInput) {
      PercentileTDigestAccumulator.SerializedTDigestInput input =
          new PercentileTDigestAccumulator.SerializedTDigestInput();
      input.reset(bytes);
      accumulator = PercentileTDigestAccumulator.forSerializedTDigest(input);
      accumulator.addSerializedTDigest(input);
    } else {
      accumulator = PercentileTDigestAccumulator.forSerializedTDigest(bytes);
      accumulator.addSerializedTDigest(bytes);
    }

    Arrays.fill(bytes, (byte) 0);
    Assert.assertEquals(accumulator.size(), 3L);
    Assert.assertEquals(accumulator.centroidCount(), 3);
    Assert.assertEquals(accumulator.getMin(), 0.0);
    Assert.assertEquals(accumulator.getMax(), 2.0);
    Assert.assertEquals(accumulator.serialize(), expected);
  }

  private static double serializedValue(int digestIndex, int valueIndex, int numDigests, int valuesPerDigest) {
    return (valueIndex * (double) numDigests + digestIndex) / (numDigests * (double) valuesPerDigest);
  }

  private static TDigest aggregateSerialized(byte[][] values, RoaringBitmap nullBitmap, boolean nullHandlingEnabled) {
    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, nullHandlingEnabled);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(values.length, resultHolder, Map.of(EXPRESSION, bytesBlockValSet(values, nullBitmap)));
    return function.extractAggregationResult(resultHolder);
  }

  private static void assertSerializedAccumulatorState(GroupByResultHolder resultHolder, int groupKey) {
    Assert.assertTrue(resultHolder.getResult(groupKey) instanceof PercentileTDigestAccumulator);
  }

  private static void assertGroupResult(PercentileTDigestAggregationFunction function,
      GroupByResultHolder resultHolder, int groupKey, long expectedSize, double expectedCompression,
      double expectedMin, double expectedMax) {
    TDigest result = function.extractGroupByResult(resultHolder, groupKey);
    Assert.assertTrue(result instanceof PercentileTDigestAccumulator);
    Assert.assertEquals(result.size(), expectedSize);
    Assert.assertEquals(result.compression(), expectedCompression);
    Assert.assertEquals(result.getMin(), expectedMin);
    Assert.assertEquals(result.getMax(), expectedMax);
    Assert.assertEquals(result.quantile(0.75), 0.75, 0.02);
    Assert.assertTrue(((PercentileTDigestAccumulator) result).toTDigest() instanceof MergingDigest);
  }

  private static void assertValidReducerResult(TDigest result, double[] sortedValues, String caseDescription) {
    assertValidReducerResult(result, sortedValues, caseDescription, 0.02);
  }

  private static void assertValidReducerResult(TDigest result, double[] sortedValues, String caseDescription,
      double maxRankError) {
    double[] maxRankErrors = new double[6];
    Arrays.fill(maxRankErrors, maxRankError);
    assertValidReducerResult(result, sortedValues, caseDescription, maxRankErrors);
  }

  private static void assertValidReducerResult(TDigest result, double[] sortedValues, String caseDescription,
      double[] maxRankErrors) {
    double[] rankErrors = getReducerRankErrors(result, sortedValues, caseDescription);
    double[] quantiles = {0.0, 0.5, 0.75, 0.95, 0.99, 1.0};
    for (int i = 0; i < quantiles.length; i++) {
      Assert.assertTrue(rankErrors[i] <= maxRankErrors[i],
          "Rank error " + rankErrors[i] + " exceeds the t-digest 3.3 reference envelope for p"
              + quantiles[i] * 100 + " in " + caseDescription);
    }
  }

  private static double[] getReducerRankErrors(TDigest result, double[] sortedValues, String caseDescription) {
    Assert.assertEquals(result.size(), sortedValues.length, "Total weight differs for " + caseDescription);
    long centroidWeight = 0L;
    double previousMean = Double.NEGATIVE_INFINITY;
    for (Centroid centroid : result.centroids()) {
      Assert.assertTrue(Double.isFinite(centroid.mean()), "Non-finite centroid mean for " + caseDescription);
      Assert.assertTrue(centroid.count() > 0, "Non-positive centroid weight for " + caseDescription);
      Assert.assertTrue(centroid.mean() + 1e-12 >= previousMean,
          "Centroids are not sorted for " + caseDescription);
      centroidWeight += centroid.count();
      previousMean = centroid.mean();
    }
    Assert.assertEquals(centroidWeight, sortedValues.length,
        "Centroid weights do not sum to the input size for " + caseDescription);

    double[] quantiles = {0.0, 0.5, 0.75, 0.95, 0.99, 1.0};
    double[] rankErrors = new double[quantiles.length];
    double previousQuantile = Double.NEGATIVE_INFINITY;
    for (int i = 0; i < quantiles.length; i++) {
      double quantile = quantiles[i];
      double value = result.quantile(quantile);
      Assert.assertTrue(Double.isFinite(value), "Non-finite p" + quantile * 100 + " for " + caseDescription);
      Assert.assertTrue(value >= previousQuantile, "Quantiles are not monotonic for " + caseDescription);
      previousQuantile = value;
      if (quantile == 0.0) {
        Assert.assertEquals(value, sortedValues[0], "Incorrect minimum for " + caseDescription);
      } else if (quantile == 1.0) {
        Assert.assertEquals(value, sortedValues[sortedValues.length - 1],
            "Incorrect maximum for " + caseDescription);
      } else {
        double rankValue = snapToObservedValue(sortedValues, value);
        int lowerRank = lowerBound(sortedValues, rankValue);
        int upperRank = upperBound(sortedValues, rankValue);
        rankErrors[i] = Math.max(0.0, Math.max(lowerRank / (double) sortedValues.length - quantile,
            quantile - upperRank / (double) sortedValues.length));
      }
    }
    return rankErrors;
  }

  private static double snapToObservedValue(double[] sortedValues, double value) {
    int insertionPoint = lowerBound(sortedValues, value);
    if (insertionPoint < sortedValues.length && nearlyEqual(value, sortedValues[insertionPoint])) {
      return sortedValues[insertionPoint];
    }
    if (insertionPoint > 0 && nearlyEqual(value, sortedValues[insertionPoint - 1])) {
      return sortedValues[insertionPoint - 1];
    }
    return value;
  }

  private static boolean nearlyEqual(double first, double second) {
    return Math.abs(first - second) <= 1e-12 * Math.max(1.0, Math.max(Math.abs(first), Math.abs(second)));
  }

  private static void assertDuplicateInfinityResult(TDigest result, long expectedSize) {
    Assert.assertEquals(result.size(), expectedSize);
    Assert.assertEquals(result.getMin(), Double.NEGATIVE_INFINITY);
    Assert.assertEquals(result.getMax(), Double.POSITIVE_INFINITY);
    for (Centroid centroid : result.centroids()) {
      Assert.assertFalse(Double.isNaN(centroid.mean()));
      Assert.assertTrue(centroid.count() > 0);
    }
    Assert.assertEquals(result.quantile(0.0), Double.NEGATIVE_INFINITY);
    Assert.assertEquals(result.quantile(0.5), 0.0);
    Assert.assertEquals(result.quantile(1.0), Double.POSITIVE_INFINITY);
  }

  private static TDigest createReducerSource(double[] values, int compression, ReducerInput reducerInput) {
    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, compression, false);
    if (reducerInput == ReducerInput.SERVER_LOCAL) {
      AggregationResultHolder resultHolder = function.createAggregationResultHolder();
      function.aggregate(values.length, resultHolder,
          Map.of(EXPRESSION, SyntheticBlockValSets.Double.create(null, values)));
      return function.extractAggregationResult(resultHolder);
    }

    TDigest digest = TDigest.createMergingDigest(compression);
    for (double value : values) {
      digest.add(value);
    }
    byte[] bytes = ObjectSerDeUtils.TDIGEST_SER_DE.serialize(digest);
    return function.deserializeIntermediateResult(new CustomObject(ObjectSerDeUtils.ObjectType.TDigest.getValue(),
        ByteBuffer.wrap(bytes)));
  }

  private static TDigest createReferenceReducerSource(double[] values, int compression,
      ReducerInput reducerInput) {
    TDigest digest = TDigest.createMergingDigest(compression);
    for (double value : values) {
      digest.add(value);
    }
    if (reducerInput == ReducerInput.DISTRIBUTED) {
      return ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(ObjectSerDeUtils.TDIGEST_SER_DE.serialize(digest));
    }
    return digest;
  }

  private static int lowerBound(double[] sortedValues, double value) {
    int low = 0;
    int high = sortedValues.length;
    while (low < high) {
      int middle = (low + high) >>> 1;
      if (sortedValues[middle] < value) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  private static int upperBound(double[] sortedValues, double value) {
    int low = 0;
    int high = sortedValues.length;
    while (low < high) {
      int middle = (low + high) >>> 1;
      if (sortedValues[middle] <= value) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  private enum ReducerDistribution {
    UNIFORM {
      @Override
      double value(double unitValue) {
        return unitValue;
      }
    },
    SKEWED {
      @Override
      double value(double unitValue) {
        return Math.pow(unitValue, 8.0);
      }
    },
    HEAVY_TAIL {
      @Override
      double value(double unitValue) {
        return Math.pow(1.0 - 0.999 * unitValue, -1.5) - 1.0;
      }
    },
    BIMODAL {
      @Override
      double value(double unitValue) {
        return unitValue < 0.5 ? 0.1 + 0.2 * unitValue : 0.7 + 0.2 * unitValue;
      }
    },
    DUPLICATE_HEAVY {
      @Override
      double value(double unitValue) {
        return unitValue < 0.7 ? 0.1 : unitValue < 0.9 ? 0.5 : 0.9;
      }
    };

    abstract double value(double unitValue);
  }

  private enum ReducerInput {
    SERVER_LOCAL,
    DISTRIBUTED
  }

  private static void setAccumulatorField(PercentileTDigestAccumulator accumulator, String name, Object value)
      throws ReflectiveOperationException {
    Field field = PercentileTDigestAccumulator.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(accumulator, value);
  }

  private static Object getAccumulatorField(PercentileTDigestAccumulator accumulator, String name)
      throws ReflectiveOperationException {
    Field field = PercentileTDigestAccumulator.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(accumulator);
  }

  private enum MergeOrder {
    ORIGINAL {
      @Override
      int[] sourceIndexes(int fanIn) {
        int[] indexes = new int[fanIn];
        for (int i = 0; i < fanIn; i++) {
          indexes[i] = i;
        }
        return indexes;
      }
    },
    REVERSED {
      @Override
      int[] sourceIndexes(int fanIn) {
        int[] indexes = ORIGINAL.sourceIndexes(fanIn);
        for (int i = 0; i < fanIn / 2; i++) {
          int otherIndex = fanIn - i - 1;
          int value = indexes[i];
          indexes[i] = indexes[otherIndex];
          indexes[otherIndex] = value;
        }
        return indexes;
      }
    },
    RANDOMIZED {
      @Override
      int[] sourceIndexes(int fanIn) {
        int[] indexes = ORIGINAL.sourceIndexes(fanIn);
        SplittableRandom random = new SplittableRandom(0xBADC0FFEE0DDF00DL + fanIn);
        for (int i = indexes.length - 1; i > 0; i--) {
          int otherIndex = random.nextInt(i + 1);
          int value = indexes[i];
          indexes[i] = indexes[otherIndex];
          indexes[otherIndex] = value;
        }
        return indexes;
      }
    };

    abstract int[] sourceIndexes(int fanIn);
  }

  private static BlockValSet bytesBlockValSet(byte[][] values, RoaringBitmap nullBitmap) {
    return new SyntheticBlockValSets.Base() {
      @Override
      public RoaringBitmap getNullBitmap() {
        return nullBitmap;
      }

      @Override
      public DataType getValueType() {
        return DataType.BYTES;
      }

      @Override
      public boolean isSingleValue() {
        return true;
      }

      @Override
      public byte[][] getBytesValuesSV() {
        return values;
      }
    };
  }
}
