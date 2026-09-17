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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction.SerializedIntermediateResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.mockito.Mockito;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Covers the serialized-bitmap (`BYTES`) aggregation paths of [DistinctCountBitmapAggregationFunction], which union
/// input bitmaps lazily and repair the accumulator at extraction. The input cardinalities are chosen to cross the
/// container thresholds of the lazy union (array containers promote to bitmap containers past 1024 combined
/// cardinality; repair converts back to array containers at up to 4096), so both promoted and non-promoted
/// accumulator states are verified against an eagerly unioned reference. Intermediate-result merges must remain
/// valid for cardinality reads and serialization immediately after every merge.
public class DistinctCountBitmapLazyUnionTest {
  private static final ExpressionContext EXPRESSION = ExpressionContext.forIdentifier("bitmapCol");
  private static final Random RANDOM = new Random(42);

  private static byte[][] serializedBitmaps(int numBitmaps, int valuesPerBitmap, int maxValue) {
    byte[][] serialized = new byte[numBitmaps][];
    for (int i = 0; i < numBitmaps; i++) {
      RoaringBitmap bitmap = new RoaringBitmap();
      for (int j = 0; j < valuesPerBitmap; j++) {
        bitmap.add(RANDOM.nextInt(maxValue));
      }
      serialized[i] = RoaringBitmapUtils.serialize(bitmap);
    }
    return serialized;
  }

  private static RoaringBitmap eagerUnion(byte[][] serialized, int from, int to) {
    RoaringBitmap expected = new RoaringBitmap();
    for (int i = from; i < to; i++) {
      expected.or(RoaringBitmapUtils.deserialize(serialized[i]));
    }
    return expected;
  }

  private static Map<ExpressionContext, BlockValSet> mockBlockValSetMap(byte[][] serialized) {
    BlockValSet blockValSet = Mockito.mock(BlockValSet.class);
    Mockito.when(blockValSet.getValueType()).thenReturn(DataType.BYTES);
    Mockito.when(blockValSet.isSingleValue()).thenReturn(true);
    Mockito.when(blockValSet.getBytesValuesSV()).thenReturn(serialized);
    return Collections.singletonMap(EXPRESSION, blockValSet);
  }

  @Test
  public void testAggregateAcrossBlocks() {
    // Enough overlap-heavy inputs to promote accumulator containers to (lazy) bitmap containers, split into
    // multiple aggregate() calls to verify the accumulator stays valid across blocks until extraction
    byte[][] serialized = serializedBitmaps(200, 100, 200_000);
    DistinctCountBitmapAggregationFunction function =
        new DistinctCountBitmapAggregationFunction(EXPRESSION, false);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    int blockSize = 50;
    for (int from = 0; from < serialized.length; from += blockSize) {
      byte[][] block = new byte[blockSize][];
      System.arraycopy(serialized, from, block, 0, blockSize);
      function.aggregate(blockSize, holder, mockBlockValSetMap(block));
    }

    RoaringBitmap result = function.extractAggregationResult(holder);
    RoaringBitmap expected = eagerUnion(serialized, 0, serialized.length);
    assertEquals(result, expected);
    assertEquals(result.getCardinality(), expected.getCardinality());
    // The repaired accumulator must serialize into a form that round-trips
    assertEquals(RoaringBitmapUtils.deserialize(RoaringBitmapUtils.serialize(result)), expected);
  }

  @Test
  public void testAggregateGroupBySV() {
    int numGroups = 4;
    byte[][] serialized = serializedBitmaps(400, 50, 100_000);
    int[] groupKeys = new int[serialized.length];
    for (int i = 0; i < groupKeys.length; i++) {
      groupKeys[i] = i % numGroups;
    }

    DistinctCountBitmapAggregationFunction function =
        new DistinctCountBitmapAggregationFunction(EXPRESSION, false);
    GroupByResultHolder holder = function.createGroupByResultHolder(numGroups, numGroups);
    function.aggregateGroupBySV(serialized.length, groupKeys, holder, mockBlockValSetMap(serialized));

    for (int groupKey = 0; groupKey < numGroups; groupKey++) {
      RoaringBitmap expected = new RoaringBitmap();
      for (int i = groupKey; i < serialized.length; i += numGroups) {
        expected.or(RoaringBitmapUtils.deserialize(serialized[i]));
      }
      RoaringBitmap result = function.extractGroupByResult(holder, groupKey);
      assertEquals(result, expected, "group " + groupKey);
      assertEquals(result.getCardinality(), expected.getCardinality(), "group " + groupKey);
    }
  }

  @Test
  public void testAggregateGroupByMV() {
    // Every row belongs to multiple groups, so the same deserialized input bitmap is unioned into one group's
    // accumulator and cloned as another group's initial accumulator — verifies no aliasing between accumulators
    int numGroups = 3;
    byte[][] serialized = serializedBitmaps(150, 80, 150_000);
    int[][] groupKeys = new int[serialized.length][];
    for (int i = 0; i < groupKeys.length; i++) {
      groupKeys[i] = new int[]{i % numGroups, (i + 1) % numGroups};
    }

    DistinctCountBitmapAggregationFunction function =
        new DistinctCountBitmapAggregationFunction(EXPRESSION, false);
    GroupByResultHolder holder = function.createGroupByResultHolder(numGroups, numGroups);
    function.aggregateGroupByMV(serialized.length, groupKeys, holder, mockBlockValSetMap(serialized));

    for (int groupKey = 0; groupKey < numGroups; groupKey++) {
      RoaringBitmap expected = new RoaringBitmap();
      for (int i = 0; i < serialized.length; i++) {
        if (i % numGroups == groupKey || (i + 1) % numGroups == groupKey) {
          expected.or(RoaringBitmapUtils.deserialize(serialized[i]));
        }
      }
      RoaringBitmap result = function.extractGroupByResult(holder, groupKey);
      assertEquals(result, expected, "group " + groupKey);
      assertEquals(result.getCardinality(), expected.getCardinality(), "group " + groupKey);
    }
  }

  @Test
  public void testSmallBitmapsStayCorrectBelowPromotionThreshold() {
    // All containers stay small array containers: the lazy path must degrade to plain array unions
    byte[][] serialized = serializedBitmaps(50, 5, 1_000);
    DistinctCountBitmapAggregationFunction function =
        new DistinctCountBitmapAggregationFunction(EXPRESSION, false);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(serialized.length, holder, mockBlockValSetMap(serialized));

    RoaringBitmap expected = eagerUnion(serialized, 0, serialized.length);
    assertEquals(function.extractAggregationResult(holder), expected);
  }

  @Test
  public void testMergeSparseBitmapsAcrossUnsignedRange() {
    DistinctCountBitmapAggregationFunction function =
        new DistinctCountBitmapAggregationFunction(EXPRESSION, false);
    Random random = new Random(73);
    RoaringBitmap result = new RoaringBitmap();
    RoaringBitmap expected = new RoaringBitmap();
    for (int i = 0; i < 32; i++) {
      RoaringBitmap input = RoaringBitmap.bitmapOf(0, Integer.MAX_VALUE, Integer.MIN_VALUE, -1);
      for (int j = 0; j < 128; j++) {
        input.add(random.nextInt());
      }
      RoaringBitmap originalInput = input.clone();
      expected.or(input);
      result = function.merge(result, input);
      assertMergeResult(function, result, expected);
      assertEquals(input, originalInput);
    }

    // Match two keys, then encounter interleaved missing keys and an unsigned tail at the search threshold.
    result = new RoaringBitmap();
    for (int key = 0; key < 64; key += 2) {
      result.add((key << 16) + 7);
    }
    RoaringBitmap input = RoaringBitmap.bitmapOf(13, (2 << 16) + 13, (3 << 16) + 13, (4 << 16) + 13,
        (5 << 16) + 13, (64 << 16) + 13, Integer.MIN_VALUE + 13, -1);
    RoaringBitmap originalInput = input.clone();
    expected = RoaringBitmap.or(result, input);
    assertMergeResult(function, function.merge(result, input), expected);
    assertEquals(input, originalInput);
  }

  @Test
  public void testMergeMixedContainersPreservesInputs() {
    DistinctCountBitmapAggregationFunction function =
        new DistinctCountBitmapAggregationFunction(EXPRESSION, false);
    // Three matching keys plus unrelated keys keep the search path active for each four-container input.
    RoaringBitmap result = RoaringBitmap.bitmapOf(0, 1 << 16, 2 << 16);
    for (int key = 8; key < 24; key++) {
      result.add(key << 16);
    }
    RoaringBitmap expected = result.clone();
    RoaringBitmap[] inputs = new RoaringBitmap[3];
    RoaringBitmap[] originalInputs = new RoaringBitmap[inputs.length];
    for (int i = 0; i < inputs.length; i++) {
      RoaringBitmap array = RoaringBitmap.bitmapOf(1, 17, 65_535);
      assertFalse(array.getContainerPointer().isBitmapContainer());
      assertFalse(array.getContainerPointer().isRunContainer());
      RoaringBitmap bitmap = new RoaringBitmap();
      for (int j = 0; j < 12_000; j += 2) {
        bitmap.add(j);
      }
      assertTrue(bitmap.getContainerPointer().isBitmapContainer());
      RoaringBitmap run = RoaringBitmap.bitmapOfRange(100, 10_000);
      run.runOptimize();
      assertTrue(run.getContainerPointer().isRunContainer());

      // Rotate the representations across the same keys so merges cross container types.
      RoaringBitmap input = RoaringBitmap.addOffset(array, (long) (i % 3) << 16);
      input.or(RoaringBitmap.addOffset(bitmap, (long) ((i + 1) % 3) << 16));
      input.or(RoaringBitmap.addOffset(run, (long) ((i + 2) % 3) << 16));
      input.add(((i + 4) << 16) + 7);
      inputs[i] = input;
      originalInputs[i] = input.clone();
      expected.or(input);
      result = function.merge(result, input);
      assertMergeResult(function, result, expected);
    }
    for (int i = 0; i < inputs.length; i++) {
      assertEquals(inputs[i], originalInputs[i]);
      // A key present in only this input must also have independent container ownership.
      inputs[i].add(((i + 4) << 16) + 19);
    }
    assertMergeResult(function, result, expected);
  }

  @Test
  public void testMergeEmptyAndSelf() {
    DistinctCountBitmapAggregationFunction function =
        new DistinctCountBitmapAggregationFunction(EXPRESSION, false);
    RoaringBitmap result = new RoaringBitmap();
    assertMergeResult(function, function.merge(result, result), new RoaringBitmap());
    result.add(0L, 10_000L);
    result.add(Integer.MIN_VALUE);
    result.add(-1);
    RoaringBitmap expected = result.clone();
    result = function.merge(result, result);
    assertMergeResult(function, result, expected);
    assertMergeResult(function, function.merge(result, new RoaringBitmap()), expected);
  }

  private static void assertMergeResult(DistinctCountBitmapAggregationFunction function, RoaringBitmap result,
      RoaringBitmap expected) {
    assertEquals(result, expected);
    assertEquals(result.getCardinality(), expected.getCardinality());
    assertEquals(function.extractFinalResult(result).intValue(), expected.getCardinality());
    SerializedIntermediateResult serialized = function.serializeIntermediateResult(result);
    RoaringBitmap roundTripped = function.deserializeIntermediateResult(
        new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes())));
    assertEquals(roundTripped, expected);
    assertEquals(roundTripped.getCardinality(), expected.getCardinality());
  }
}
