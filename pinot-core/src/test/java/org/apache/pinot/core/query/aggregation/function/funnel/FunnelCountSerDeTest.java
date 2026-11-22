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
package org.apache.pinot.core.query.aggregation.function.funnel;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction.SerializedResult;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Checks the wire format of the per-step states of FUNNEL_COUNT: the `List` framing of the step bitmaps, sets and
/// sketches, where an empty list is its size alone, and the step counts of the partitioned mode.
@SuppressWarnings({"rawtypes", "unchecked"})
public class FunnelCountSerDeTest {
  private static final int LIST_TYPE = 21;
  private static final int LONG_ARRAY_LIST_TYPE = 43;

  @Test
  public void testEmptyStepsSerializeToSizeOnly() {
    AggregationFunction function = funnelCount();
    SerializedResult serialized = function.serializeIntermediateResult(List.of());
    assertEquals(serialized.getType(), LIST_TYPE);
    assertEquals(serialized.getBytes(), new byte[Integer.BYTES]);
    CustomObject customObject = new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes()));
    assertEquals(function.deserializeIntermediateResult(customObject), List.of());
  }

  /// Step bitmaps go out as `[int size][int ROARING_BITMAP_TYPE]`, then `[int length]` and the portable bitmap bytes
  /// per step.
  @Test
  public void testBitmapSteps() {
    byte[] bytes = HexFormat.of().parseHex("00000002" + "0000000e"
        + "00000014" + "3a30000001000000000001001000000001000200"
        + "00000012" + "3a3000000100000000000000100000000200");
    assertListSteps(funnelCount(), List.of(RoaringBitmap.bitmapOf(1, 2), RoaringBitmap.bitmapOf(2)), bytes,
        new long[]{2, 1});
  }

  /// Step sets go out as `[int size][int set type]`, then `[int length]` and the set bytes per step. The set type
  /// follows the stored type of the correlation column, and an empty step set is a string set.
  @Test
  public void testSetSteps() {
    AggregationFunction function = funnelCount("set");
    byte[] intSets = HexFormat.of().parseHex("00000002" + "00000009"
        + "00000008" + "0000000100000007"
        + "00000008" + "0000000100000007");
    assertListSteps(function, List.of(new IntOpenHashSet(new int[]{7}), new IntOpenHashSet(new int[]{7})), intSets,
        new long[]{1, 1});
    byte[] stringSets = HexFormat.of().parseHex("00000002" + "00000012"
        + "00000009" + "000000010000000161"
        + "00000004" + "00000000");
    assertListSteps(function, List.of(new ObjectOpenHashSet<>(List.of("a")), new ObjectOpenHashSet<>()), stringSets,
        new long[]{1, 0});
  }

  /// A segment without rows contributes empty step sets whatever the stored type of the correlation column. Merged on
  /// either side of typed step sets, they leave the typed sets, which go out with the set type of that stored type.
  @Test
  public void testMergeSetStepsWithEmptySegment() {
    AggregationFunction function = funnelCount("set");
    AggregationResultHolder emptySegment = function.createAggregationResultHolder();
    List intSteps = List.of(new IntOpenHashSet(new int[]{7}), new IntOpenHashSet());
    byte[] intSets = HexFormat.of().parseHex("00000002" + "00000009"
        + "00000008" + "0000000100000007"
        + "00000004" + "00000000");
    assertListSteps(function, (List) function.merge(function.extractAggregationResult(emptySegment), intSteps),
        intSets, new long[]{1, 0});
    assertListSteps(function, (List) function.merge(intSteps, function.extractAggregationResult(emptySegment)),
        intSets, new long[]{1, 0});
  }

  /// Step sketches go out as `[int size][int DATA_SKETCH_TYPE]`, then `[int length]` and the compact sketch bytes per
  /// step.
  @Test
  public void testThetaSketchSteps() {
    UpdatableThetaSketch sketch = UpdatableThetaSketch.builder().build();
    sketch.update(1L);
    byte[] bytes = HexFormat.of().parseHex("00000002" + "0000000c"
        + "00000010" + "01030300003acc9315f97dcbbd86a105"
        + "00000008" + "01030300001e0000");
    assertListSteps(funnelCount("theta_sketch"), List.of(sketch, UpdatableThetaSketch.builder().build()), bytes,
        new long[]{1, 0});
  }

  /// Step counts in a `LongArrayList` go out as the `LongArrayList` wire type, `[int size]` then the counts. Any other
  /// list goes out as the `List` wire type, `[int size][int LONG_TYPE]` then `[int Long.BYTES][long count]` per step.
  @Test
  public void testPartitionedStepCounts() {
    for (AggregationFunction function : List.of(funnelCount("partitioned"), funnelCount("partitioned", "sorted"))) {
      SerializedResult longArrayList = function.serializeIntermediateResult(LongArrayList.wrap(new long[]{2, 1}));
      assertEquals(longArrayList.getType(), LONG_ARRAY_LIST_TYPE);
      assertEquals(longArrayList.getBytes(),
          HexFormat.of().parseHex("00000002" + "0000000000000002" + "0000000000000001"));
      SerializedResult list = function.serializeIntermediateResult(List.of(0L, 3L));
      assertEquals(list.getType(), LIST_TYPE);
      byte[] listBytes = HexFormat.of().parseHex("00000002" + "00000001" + "000000080000000000000000"
          + "000000080000000000000003");
      assertEquals(list.getBytes(), listBytes);
      CustomObject longArrayListObject =
          new CustomObject(LONG_ARRAY_LIST_TYPE, ByteBuffer.wrap(longArrayList.getBytes()));
      assertEquals(function.extractFinalResult(function.deserializeIntermediateResult(longArrayListObject)),
          new long[]{2, 1});
      CustomObject listObject = new CustomObject(LIST_TYPE, ByteBuffer.wrap(listBytes));
      assertEquals(function.extractFinalResult(function.deserializeIntermediateResult(listObject)), new long[]{0, 3});
    }
  }

  /// Checks that `steps` serializes to `bytes` with the `List` wire type, and that `bytes` deserializes to steps whose
  /// final result is `counts`.
  private static void assertListSteps(AggregationFunction function, List<?> steps, byte[] bytes, long[] counts) {
    SerializedResult serialized = function.serializeIntermediateResult(steps);
    assertEquals(serialized.getType(), LIST_TYPE);
    assertEquals(serialized.getBytes(), bytes);
    CustomObject customObject = new CustomObject(LIST_TYPE, ByteBuffer.wrap(bytes));
    assertEquals(function.extractFinalResult(function.deserializeIntermediateResult(customObject)), counts);
  }

  private static AggregationFunction funnelCount(String... settings) {
    List<ExpressionContext> arguments = new ArrayList<>(List.of(
        ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "steps",
            List.of(ExpressionContext.forIdentifier("step0"), ExpressionContext.forIdentifier("step1")))),
        ExpressionContext.forFunction(new FunctionContext(FunctionContext.Type.TRANSFORM, "correlateby",
            List.of(ExpressionContext.forIdentifier("userId"))))));
    if (settings.length > 0) {
      List<ExpressionContext> literals = new ArrayList<>(settings.length);
      for (String setting : settings) {
        literals.add(ExpressionContext.forLiteral(Literal.stringValue(setting)));
      }
      arguments.add(ExpressionContext.forFunction(
          new FunctionContext(FunctionContext.Type.TRANSFORM, "settings", literals)));
    }
    return new FunnelCountAggregationFunctionFactory(arguments, false).get();
  }
}
