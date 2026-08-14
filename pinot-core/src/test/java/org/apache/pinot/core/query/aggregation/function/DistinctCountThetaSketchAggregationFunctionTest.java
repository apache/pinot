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

import java.util.List;
import java.util.Map;
import org.apache.datasketches.theta.UpdatableThetaSketch;
import org.apache.datasketches.theta.UpdatableThetaSketchBuilder;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.Constants;
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


public class DistinctCountThetaSketchAggregationFunctionTest {
  private static final ExpressionContext UUID_EXPRESSION = ExpressionContext.forIdentifier("uuidCol");
  private static final ExpressionContext BYTES_EXPRESSION = ExpressionContext.forIdentifier("bytesCol");
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
  public void testCanUseStarTreeDefaultK() {
    // Default aggregation function lgK = 12 / K=4096
    DistinctCountThetaSketchAggregationFunction function =
        new DistinctCountThetaSketchAggregationFunction(List.of(ExpressionContext.forIdentifier("col")));

    Assert.assertTrue(function.canUseStarTree(Map.of()));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "4096")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, 4096)));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, 2048)));
  }

  @Test
  public void testCanUseCustomK() {
    DistinctCountThetaSketchAggregationFunction function = new DistinctCountThetaSketchAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"),
            ExpressionContext.forLiteral(Literal.stringValue("nominalEntries=32768"))));

    // Default StarTree lgK = 14 / K=16384
    Assert.assertFalse(function.canUseStarTree(Map.of()));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "16384")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "65536")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, 32768)));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "32768")));
  }

  @Test(dataProvider = "uuidValueModes")
  public void testAggregateUuid(boolean singleValue) {
    DistinctCountThetaSketchAggregationFunction function =
        new DistinctCountThetaSketchAggregationFunction(List.of(UUID_EXPRESSION));
    BlockValSet blockValSet = mockUuidBlockValSet(singleValue);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();

    function.aggregate(UUID_VALUES_SV.length, resultHolder, Map.of(UUID_EXPRESSION, blockValSet));

    Assert.assertEquals(extractFinalResult(function, resultHolder), singleValue ? 3L : 4L);
    verifyBytesAccessor(blockValSet, singleValue);
  }

  @Test(dataProvider = "uuidValueModes")
  public void testAggregateUuidGroupBySV(boolean singleValue) {
    DistinctCountThetaSketchAggregationFunction function =
        new DistinctCountThetaSketchAggregationFunction(List.of(UUID_EXPRESSION));
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
    DistinctCountThetaSketchAggregationFunction function =
        new DistinctCountThetaSketchAggregationFunction(List.of(UUID_EXPRESSION));
    BlockValSet blockValSet = mockUuidBlockValSet(singleValue);
    GroupByResultHolder resultHolder = function.createGroupByResultHolder(2, 2);

    function.aggregateGroupByMV(UUID_VALUES_SV.length, new int[][]{{0}, {1}, {0, 1}, {1}}, resultHolder,
        Map.of(UUID_EXPRESSION, blockValSet));

    Assert.assertEquals(extractFinalResult(function, resultHolder, 0), singleValue ? 1L : 2L);
    Assert.assertEquals(extractFinalResult(function, resultHolder, 1), singleValue ? 3L : 4L);
    verifyBytesAccessor(blockValSet, singleValue);
  }

  @Test
  public void testUuidPredicateUsesLogicalType() {
    DistinctCountThetaSketchAggregationFunction function = new DistinctCountThetaSketchAggregationFunction(
        List.of(UUID_EXPRESSION, ExpressionContext.forLiteral(Literal.stringValue("")),
            ExpressionContext.forLiteral(Literal.stringValue("uuidCol = '550e8400-e29b-41d4-a716-446655440000'")),
            ExpressionContext.forLiteral(Literal.stringValue("$1"))));
    BlockValSet blockValSet = mockUuidBlockValSet(true);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();

    function.aggregate(UUID_VALUES_SV.length, resultHolder, Map.of(UUID_EXPRESSION, blockValSet));

    Assert.assertEquals(extractFinalResult(function, resultHolder), 1L);
  }

  @Test
  public void testAggregateMultiValueBytesAsRawValues() {
    // These one-byte values are not serialized sketches, so the test also verifies that MV BYTES are not deserialized.
    byte[][][] bytesValues = {
        {{1}, {2}},
        {{2}, {3}},
        {{1}},
        {{4}}
    };
    BlockValSet blockValSet = mock(BlockValSet.class);
    when(blockValSet.getValueType()).thenReturn(DataType.BYTES);
    when(blockValSet.isSingleValue()).thenReturn(false);
    when(blockValSet.getBytesValuesMV()).thenReturn(bytesValues);
    DistinctCountThetaSketchAggregationFunction function =
        new DistinctCountThetaSketchAggregationFunction(List.of(BYTES_EXPRESSION));

    AggregationResultHolder aggregationResultHolder = function.createAggregationResultHolder();
    function.aggregate(bytesValues.length, aggregationResultHolder, Map.of(BYTES_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, aggregationResultHolder), 4L);

    GroupByResultHolder groupBySVResultHolder = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupBySV(bytesValues.length, new int[]{0, 0, 1, 1}, groupBySVResultHolder,
        Map.of(BYTES_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, groupBySVResultHolder, 0), 3L);
    Assert.assertEquals(extractFinalResult(function, groupBySVResultHolder, 1), 2L);

    GroupByResultHolder groupByMVResultHolder = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupByMV(bytesValues.length, new int[][]{{0}, {1}, {0, 1}, {1}}, groupByMVResultHolder,
        Map.of(BYTES_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, groupByMVResultHolder, 0), 2L);
    Assert.assertEquals(extractFinalResult(function, groupByMVResultHolder, 1), 4L);
    verify(blockValSet, atLeastOnce()).getBytesValuesMV();
    verify(blockValSet, never()).getBytesValuesSV();
  }

  @Test
  public void testAggregateSingleValueSerializedSketches() {
    byte[][] serializedSketches = {
        serializedSketch("a", "b"),
        serializedSketch("b", "c"),
        new byte[0],
        serializedSketch("d")
    };
    BlockValSet blockValSet = mock(BlockValSet.class);
    when(blockValSet.getValueType()).thenReturn(DataType.BYTES);
    when(blockValSet.isSingleValue()).thenReturn(true);
    when(blockValSet.getBytesValuesSV()).thenReturn(serializedSketches);
    DistinctCountThetaSketchAggregationFunction function =
        new DistinctCountThetaSketchAggregationFunction(List.of(BYTES_EXPRESSION));

    AggregationResultHolder aggregationResultHolder = function.createAggregationResultHolder();
    function.aggregate(serializedSketches.length, aggregationResultHolder, Map.of(BYTES_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, aggregationResultHolder), 4L);

    GroupByResultHolder groupBySVResultHolder = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupBySV(serializedSketches.length, new int[]{0, 0, 1, 1}, groupBySVResultHolder,
        Map.of(BYTES_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, groupBySVResultHolder, 0), 3L);
    Assert.assertEquals(extractFinalResult(function, groupBySVResultHolder, 1), 1L);

    GroupByResultHolder groupByMVResultHolder = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupByMV(serializedSketches.length, new int[][]{{0}, {1}, {0, 1}, {1}},
        groupByMVResultHolder, Map.of(BYTES_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, groupByMVResultHolder, 0), 2L);
    Assert.assertEquals(extractFinalResult(function, groupByMVResultHolder, 1), 3L);
    verify(blockValSet, atLeastOnce()).getBytesValuesSV();
    verify(blockValSet, never()).getBytesValuesMV();
  }

  private static BlockValSet mockUuidBlockValSet(boolean singleValue) {
    BlockValSet blockValSet = mock(BlockValSet.class);
    when(blockValSet.getValueType()).thenReturn(DataType.UUID);
    when(blockValSet.isSingleValue()).thenReturn(singleValue);
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

  private static byte[] serializedSketch(String... values) {
    UpdatableThetaSketch sketch = new UpdatableThetaSketchBuilder().build();
    for (String value : values) {
      sketch.update(value);
    }
    return sketch.compact().toByteArray();
  }

  private static long extractFinalResult(DistinctCountThetaSketchAggregationFunction function,
      AggregationResultHolder resultHolder) {
    return ((Number) function.extractFinalResult(function.extractAggregationResult(resultHolder))).longValue();
  }

  private static long extractFinalResult(DistinctCountThetaSketchAggregationFunction function,
      GroupByResultHolder resultHolder, int groupKey) {
    return ((Number) function.extractFinalResult(function.extractGroupByResult(resultHolder, groupKey))).longValue();
  }
}
