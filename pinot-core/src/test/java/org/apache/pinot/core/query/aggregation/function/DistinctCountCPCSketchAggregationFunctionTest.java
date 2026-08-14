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

import java.lang.foreign.MemorySegment;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.customobject.CpcSketchAccumulator;
import org.apache.pinot.segment.local.customobject.SerializedCPCSketch;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class DistinctCountCPCSketchAggregationFunctionTest {
  private static final ExpressionContext UUID_EXPRESSION = ExpressionContext.forIdentifier("uuidCol");
  private static final byte[] UUID_0 = UuidUtils.toBytes("550e8400-e29b-41d4-a716-446655440000");
  private static final byte[] UUID_1 = UuidUtils.toBytes("550e8400-e29b-41d4-a716-446655440001");
  private static final byte[] UUID_2 = UuidUtils.toBytes("550e8400-e29b-41d4-a716-446655440002");
  private static final byte[] UUID_3 = UuidUtils.toBytes("550e8400-e29b-41d4-a716-446655440003");
  private static final byte[][] UUID_VALUES_SV = {UUID_0, UUID_1, UUID_0, UUID_2};
  private static final byte[][][] UUID_VALUES_MV = {{UUID_0, UUID_1}, {UUID_1, UUID_2}, {UUID_0}, {UUID_3}};

  @DataProvider(name = "uuidValueModes")
  public static Object[][] uuidValueModes() {
    return new Object[][]{{true}, {false}};
  }

  @Test
  public void testCanUseStarTreeDefaultLgK() {
    DistinctCountCPCSketchAggregationFunction function =
        new DistinctCountCPCSketchAggregationFunction(List.of(ExpressionContext.forIdentifier("col")));

    Assert.assertTrue(function.canUseStarTree(Map.of()));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, "12")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, 12)));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, 11)));

    function = new DistinctCountCPCSketchAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"), ExpressionContext.forLiteral(Literal.intValue(12))));

    Assert.assertTrue(function.canUseStarTree(Map.of()));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, "12")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, 12)));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, 11)));
  }

  @Test
  public void testCanUseCustomLgK() {
    DistinctCountCPCSketchAggregationFunction function = new DistinctCountCPCSketchAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"),
            ExpressionContext.forLiteral(Literal.stringValue("nominalEntries=8192"))));

    // Default lgK = 12 / K=4096
    Assert.assertFalse(function.canUseStarTree(Map.of()));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, "14")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, 13)));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.CPCSKETCH_LGK_KEY, "13")));
  }

  /// Tests the empty result path used by BROKER_EVALUATE when all segments are pruned.
  /// The sequence createAggregationResultHolder → extractAggregationResult → extractFinalResult
  /// must produce a value that is convertible to the declared final result column type.
  @Test
  public void testEmptyResultProducesConvertibleFinalResult() {
    DistinctCountCPCSketchAggregationFunction function =
        new DistinctCountCPCSketchAggregationFunction(List.of(ExpressionContext.forIdentifier("col")));

    AggregationResultHolder holder = function.createAggregationResultHolder();
    CpcSketchAccumulator accumulator = function.extractAggregationResult(holder);
    Assert.assertTrue(accumulator.isEmpty());

    // extractFinalResult should return 0 for an empty accumulator
    Comparable result = function.extractFinalResult(accumulator);
    Assert.assertEquals(result, 0L);

    // The result must be convertible via the declared column type
    Object converted = function.getFinalResultColumnType().convert(result);
    Assert.assertEquals(converted, 0L);
  }

  @Test
  public void testEmptyResultProducesConvertibleRawFinalResult() {
    DistinctCountRawCPCSketchAggregationFunction function =
        new DistinctCountRawCPCSketchAggregationFunction(List.of(ExpressionContext.forIdentifier("col")));

    AggregationResultHolder holder = function.createAggregationResultHolder();
    CpcSketchAccumulator accumulator = function.extractAggregationResult(holder);
    Assert.assertTrue(accumulator.isEmpty());

    // extractFinalResult should return a SerializedCPCSketch
    SerializedCPCSketch rawResult = function.extractFinalResult(accumulator);
    Assert.assertNotNull(rawResult);

    // The declared column type is STRING; convert() must produce a String
    Assert.assertEquals(function.getFinalResultColumnType(), ColumnDataType.STRING);
    Object converted = function.getFinalResultColumnType().convert(rawResult);
    Assert.assertTrue(converted instanceof String);

    // The string should be a valid Base64-encoded CPC sketch that round-trips
    String base64 = (String) converted;
    byte[] bytes = Base64.getDecoder().decode(base64);
    CpcSketch deserialized = CpcSketch.heapify(MemorySegment.ofArray(bytes));
    Assert.assertEquals(deserialized.getEstimate(), 0.0);
  }

  @Test
  public void testMergeWithEmptyAccumulators() {
    DistinctCountCPCSketchAggregationFunction function =
        new DistinctCountCPCSketchAggregationFunction(List.of(ExpressionContext.forIdentifier("col")));

    CpcSketchAccumulator empty1 = new CpcSketchAccumulator(12, 2);
    CpcSketchAccumulator empty2 = new CpcSketchAccumulator(12, 2);
    CpcSketchAccumulator nonEmpty = new CpcSketchAccumulator(12, 2);
    CpcSketch sketch = new CpcSketch(12);
    sketch.update("hello");
    nonEmpty.apply(sketch);

    // merge(empty, non-empty) should return non-empty
    CpcSketchAccumulator result = function.merge(empty1, nonEmpty);
    Assert.assertSame(result, nonEmpty);

    // merge(non-empty, empty) should return non-empty
    result = function.merge(nonEmpty, empty2);
    Assert.assertSame(result, nonEmpty);

    // merge(empty, empty) should return empty
    result = function.merge(new CpcSketchAccumulator(12, 2), new CpcSketchAccumulator(12, 2));
    Assert.assertTrue(result.isEmpty());
  }

  @Test(dataProvider = "uuidValueModes")
  public void testAggregateUuid(boolean singleValue) {
    DistinctCountCPCSketchAggregationFunction function =
        new DistinctCountCPCSketchAggregationFunction(List.of(UUID_EXPRESSION));
    BlockValSet blockValSet = mockUuidBlockValSet(singleValue);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();

    function.aggregate(UUID_VALUES_SV.length, resultHolder, Map.of(UUID_EXPRESSION, blockValSet));

    Assert.assertEquals(extractFinalResult(function, resultHolder), singleValue ? 3L : 4L);
    verifyBytesAccessor(blockValSet, singleValue);
  }

  @Test(dataProvider = "uuidValueModes")
  public void testAggregateUuidGroupBySV(boolean singleValue) {
    DistinctCountCPCSketchAggregationFunction function =
        new DistinctCountCPCSketchAggregationFunction(List.of(UUID_EXPRESSION));
    BlockValSet blockValSet = mockUuidBlockValSet(singleValue);
    GroupByResultHolder resultHolder = function.createGroupByResultHolder(2, 2);

    function.aggregateGroupBySV(UUID_VALUES_SV.length, new int[]{0, 0, 1, 1}, resultHolder,
        Map.of(UUID_EXPRESSION, blockValSet));

    Assert.assertEquals(extractFinalResult(function, resultHolder, 0), singleValue ? 2L : 3L);
    Assert.assertEquals(extractFinalResult(function, resultHolder, 1), 2L);
    verifyBytesAccessor(blockValSet, singleValue);
  }

  @Test(dataProvider = "uuidValueModes")
  public void testAggregateUuidGroupByMV(boolean singleValue) {
    DistinctCountCPCSketchAggregationFunction function =
        new DistinctCountCPCSketchAggregationFunction(List.of(UUID_EXPRESSION));
    BlockValSet blockValSet = mockUuidBlockValSet(singleValue);
    GroupByResultHolder resultHolder = function.createGroupByResultHolder(2, 2);

    function.aggregateGroupByMV(UUID_VALUES_SV.length, new int[][]{{0}, {1}, {0, 1}, {1}}, resultHolder,
        Map.of(UUID_EXPRESSION, blockValSet));

    Assert.assertEquals(extractFinalResult(function, resultHolder, 0), singleValue ? 1L : 2L);
    Assert.assertEquals(extractFinalResult(function, resultHolder, 1), singleValue ? 3L : 4L);
    verifyBytesAccessor(blockValSet, singleValue);
  }

  @Test
  public void testAggregateDictionaryEncodedUuid() {
    DistinctCountCPCSketchAggregationFunction function =
        new DistinctCountCPCSketchAggregationFunction(List.of(UUID_EXPRESSION));
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.get(0)).thenReturn(UUID_0);
    when(dictionary.get(1)).thenReturn(UUID_1);
    when(dictionary.get(2)).thenReturn(UUID_2);

    BlockValSet blockValSet = mock(BlockValSet.class);
    when(blockValSet.getValueType()).thenReturn(DataType.UUID);
    when(blockValSet.isSingleValue()).thenReturn(true);
    when(blockValSet.isDictionaryEncoded()).thenReturn(true);
    when(blockValSet.getDictionary()).thenReturn(dictionary);
    when(blockValSet.getDictionaryIdsSV()).thenReturn(new int[]{0, 1, 0, 2});
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();

    function.aggregate(UUID_VALUES_SV.length, resultHolder, Map.of(UUID_EXPRESSION, blockValSet));

    Assert.assertEquals(extractFinalResult(function, resultHolder), 3L);
  }

  @Test
  public void testAggregateMultiValueSerializedSketches() {
    byte[][][] serializedSketches = {
        {serializedSketch("a"), serializedSketch("b")},
        {serializedSketch("b"), serializedSketch("c")},
        {new byte[0]},
        {serializedSketch("d")}
    };
    BlockValSet blockValSet = mock(BlockValSet.class);
    when(blockValSet.getValueType()).thenReturn(DataType.BYTES);
    when(blockValSet.isSingleValue()).thenReturn(false);
    when(blockValSet.isDictionaryEncoded()).thenReturn(false);
    when(blockValSet.getBytesValuesMV()).thenReturn(serializedSketches);
    DistinctCountCPCSketchAggregationFunction function =
        new DistinctCountCPCSketchAggregationFunction(List.of(UUID_EXPRESSION));

    AggregationResultHolder aggregationResultHolder = function.createAggregationResultHolder();
    function.aggregate(serializedSketches.length, aggregationResultHolder, Map.of(UUID_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, aggregationResultHolder), 4L);

    GroupByResultHolder groupBySVResultHolder = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupBySV(serializedSketches.length, new int[]{0, 0, 1, 1}, groupBySVResultHolder,
        Map.of(UUID_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, groupBySVResultHolder, 0), 3L);
    Assert.assertEquals(extractFinalResult(function, groupBySVResultHolder, 1), 1L);

    GroupByResultHolder groupByMVResultHolder = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupByMV(serializedSketches.length, new int[][]{{0}, {1}, {0, 1}, {1}},
        groupByMVResultHolder, Map.of(UUID_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, groupByMVResultHolder, 0), 2L);
    Assert.assertEquals(extractFinalResult(function, groupByMVResultHolder, 1), 3L);
    verify(blockValSet, atLeastOnce()).getBytesValuesMV();
    verify(blockValSet, never()).getBytesValuesSV();
  }

  private static BlockValSet mockUuidBlockValSet(boolean singleValue) {
    BlockValSet blockValSet = mock(BlockValSet.class);
    when(blockValSet.getValueType()).thenReturn(DataType.UUID);
    when(blockValSet.isSingleValue()).thenReturn(singleValue);
    when(blockValSet.isDictionaryEncoded()).thenReturn(false);
    if (singleValue) {
      when(blockValSet.getBytesValuesSV()).thenReturn(UUID_VALUES_SV);
    } else {
      when(blockValSet.getBytesValuesMV()).thenReturn(UUID_VALUES_MV);
    }
    return blockValSet;
  }

  private static void verifyBytesAccessor(BlockValSet blockValSet, boolean singleValue) {
    if (singleValue) {
      verify(blockValSet, atLeastOnce()).getBytesValuesSV();
      verify(blockValSet, never()).getBytesValuesMV();
    } else {
      verify(blockValSet, atLeastOnce()).getBytesValuesMV();
      verify(blockValSet, never()).getBytesValuesSV();
    }
  }

  private static byte[] serializedSketch(String value) {
    CpcSketch sketch = new CpcSketch();
    sketch.update(value);
    return sketch.toByteArray();
  }

  private static long extractFinalResult(DistinctCountCPCSketchAggregationFunction function,
      AggregationResultHolder resultHolder) {
    return ((Number) function.extractFinalResult(function.extractAggregationResult(resultHolder))).longValue();
  }

  private static long extractFinalResult(DistinctCountCPCSketchAggregationFunction function,
      GroupByResultHolder resultHolder, int groupKey) {
    return ((Number) function.extractFinalResult(function.extractGroupByResult(resultHolder, groupKey))).longValue();
  }
}
