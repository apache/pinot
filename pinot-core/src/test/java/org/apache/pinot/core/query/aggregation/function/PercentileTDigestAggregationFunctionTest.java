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
    Assert.assertTrue(Double.isNaN(result.quantile(0.0)));
    Assert.assertTrue(Double.isNaN(result.quantile(1.0)));
    Assert.assertEquals(result.quantile(0.5), 0.0);
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
    Assert.assertEquals(smallResult.quantile(0.75), verboseResult.quantile(0.75), 0.001);
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
    assertOversizedSmallIntermediateRoundTrip(function, lazyResult, numCentroids);

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
    assertOversizedSmallIntermediateRoundTrip(function, result, numCentroids);
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
    Assert.assertTrue(preciseResult.centroidCount() <= 2 * Math.ceil(preciseCompression) + 10);
  }

  private static void assertOversizedSmallIntermediateRoundTrip(PercentileTDigestAggregationFunction function,
      TDigest result, int numCentroids) {
    SerializedIntermediateResult serializedResult = function.serializeIntermediateResult(result);
    Assert.assertEquals(serializedResult.getType(), ObjectSerDeUtils.ObjectType.TDigest.getValue());
    byte[] serializedBytes = serializedResult.getBytes();
    Assert.assertEquals(ByteBuffer.wrap(serializedBytes).getInt(), 2);
    TDigest roundTripped = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(serializedBytes);
    Assert.assertEquals(roundTripped.size(), numCentroids);
    Assert.assertEquals(roundTripped.centroidCount(), numCentroids);
    Assert.assertEquals(roundTripped.compression(), 20.0);
    Assert.assertEquals(roundTripped.getMin(), 0.0);
    Assert.assertEquals(roundTripped.getMax(), numCentroids - 1.0);
    double previousValue = Double.NEGATIVE_INFINITY;
    for (double quantile : new double[]{0.0, 0.5, 0.75, 0.95, 0.99, 1.0}) {
      double value = roundTripped.quantile(quantile);
      Assert.assertTrue(Double.isFinite(value));
      Assert.assertTrue(value >= previousValue);
      Assert.assertEquals(value, result.quantile(quantile));
      previousValue = value;
    }
  }

  @Test
  public void testCapacityPreservingByteSizeRejectsUnencodableCentroidCount()
      throws ReflectiveOperationException {
    PercentileTDigestAccumulator accumulator = PercentileTDigestAccumulator.forReduction(20.0);
    int unencodableCentroidCount = Short.MAX_VALUE + 1;
    Field numCentroidsField = PercentileTDigestAccumulator.class.getDeclaredField("_numCentroids");
    numCentroidsField.setAccessible(true);
    numCentroidsField.setInt(accumulator, unencodableCentroidCount);
    Field serializedMainCapacityField =
        PercentileTDigestAccumulator.class.getDeclaredField("_serializedMainCapacity");
    serializedMainCapacityField.setAccessible(true);
    serializedMainCapacityField.setInt(accumulator, unencodableCentroidCount);

    IllegalStateException exception = Assert.expectThrows(IllegalStateException.class, accumulator::byteSize);
    Assert.assertEquals(exception.getMessage(),
        "TDigest has too many centroids for capacity-preserving encoding: " + unencodableCentroidCount);
  }

  @Test
  public void testSerializedGroupByMVSharedInputUsesCentroidCountForInitialCapacity()
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
    Assert.assertEquals(((double[]) meansField.get(input)).length, 1);
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
    int numOversizedVerboseCentroids = 51;
    ByteBuffer populatedOversizedVerbose = ByteBuffer.allocate(4 * Integer.BYTES + 2 * Double.BYTES
        + 2 * Double.BYTES * numOversizedVerboseCentroids);
    populatedOversizedVerbose.putInt(1);
    populatedOversizedVerbose.putDouble(0.0);
    populatedOversizedVerbose.putDouble(numOversizedVerboseCentroids - 1.0);
    populatedOversizedVerbose.putDouble(20.0);
    populatedOversizedVerbose.putInt(numOversizedVerboseCentroids);
    for (int i = 0; i < numOversizedVerboseCentroids; i++) {
      populatedOversizedVerbose.putDouble(1.0);
      populatedOversizedVerbose.putDouble(i);
    }
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
        {populatedOversizedVerbose.array()}, {oversizedSmall.array()}, {negativeMainCapacity.array()},
        {negativeBufferCapacity.array()}, {centroidsExceedMainCapacity.array()}
    };
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
    for (int digestIndex = 0; digestIndex < fanIn; digestIndex++) {
      SplittableRandom random = new SplittableRandom(0x5EEDL + digestIndex);
      double[] sourceValues = new double[valuesPerDigest];
      for (int valueIndex = 0; valueIndex < valuesPerDigest; valueIndex++) {
        double value = distribution.value(random.nextDouble());
        rawValues[digestIndex * valuesPerDigest + valueIndex] = value;
        sourceValues[valueIndex] = value;
      }
      sources[digestIndex] = createReducerSource(sourceValues, compression, reducerInput);
    }

    PercentileTDigestAggregationFunction function =
        new PercentileTDigestAggregationFunction(EXPRESSION, 75.0, compression, false);
    TDigest result = TDigest.createMergingDigest(compression);
    for (int sourceIndex : mergeOrder.sourceIndexes(fanIn)) {
      result = function.merge(result, sources[sourceIndex]);
    }

    Arrays.sort(rawValues);
    String caseDescription =
        fanIn + "/" + compression + "/" + distribution + "/" + mergeOrder + "/" + reducerInput;
    double maxRankError = compression == 20 ? 0.06 : 0.02;
    assertValidReducerResult(result, rawValues, caseDescription, maxRankError);

    SerializedIntermediateResult serializedResult = function.serializeIntermediateResult(result);
    Assert.assertEquals(serializedResult.getType(), ObjectSerDeUtils.ObjectType.TDigest.getValue());
    TDigest roundTripped = ObjectSerDeUtils.TDIGEST_SER_DE.deserialize(serializedResult.getBytes());
    Assert.assertTrue(roundTripped instanceof MergingDigest);
    assertValidReducerResult(roundTripped, rawValues, caseDescription + "/standard-wire", maxRankError);
    for (double quantile : new double[]{0.0, 0.5, 0.75, 0.95, 0.99, 1.0}) {
      Assert.assertEquals(result.quantile(quantile), roundTripped.quantile(quantile),
          "Wire round trip changed p" + quantile * 100 + " for " + caseDescription);
    }
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
    Assert.assertEquals(function.extractFinalResult(result), sortedValues[numRows * 75 / 100], 0.02);
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
      TDigest digest = TDigest.createMergingDigest(compression);
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

    double previousQuantile = Double.NEGATIVE_INFINITY;
    for (double quantile : new double[]{0.0, 0.5, 0.75, 0.95, 0.99, 1.0}) {
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
        int lowerRank = lowerBound(sortedValues, value);
        int upperRank = upperBound(sortedValues, value);
        double rankError = Math.max(0.0, Math.max(lowerRank / (double) sortedValues.length - quantile,
            quantile - upperRank / (double) sortedValues.length));
        Assert.assertTrue(rankError <= maxRankError,
            "Rank error " + rankError + " exceeds the frozen envelope for p" + quantile * 100 + " in "
                + caseDescription);
      }
    }
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
