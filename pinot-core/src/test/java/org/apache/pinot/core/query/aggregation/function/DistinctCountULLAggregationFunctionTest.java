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

import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.local.utils.UltraLogLogUtils;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class DistinctCountULLAggregationFunctionTest {
  private static final ExpressionContext INPUT_EXPRESSION = ExpressionContext.forIdentifier("inputCol");
  private static final byte[] UUID_0 = UuidUtils.toBytes("550e8400-e29b-41d4-a716-446655440000");
  private static final byte[] UUID_1 = UuidUtils.toBytes("550e8400-e29b-41d4-a716-446655440001");
  private static final byte[] UUID_2 = UuidUtils.toBytes("550e8400-e29b-41d4-a716-446655440002");

  @Test
  public void testCanUseStarTreeDefaultP() {
    DistinctCountULLAggregationFunction function = new DistinctCountULLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col")));

    Assert.assertTrue(function.canUseStarTree(Map.of()));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.HLLPLUS_ULL_P_KEY, "12")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.HLLPLUS_ULL_P_KEY, 12)));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.HLLPLUS_ULL_P_KEY, 16)));

    function = new DistinctCountULLAggregationFunction(List.of(ExpressionContext.forIdentifier("col"),
        ExpressionContext.forLiteral(Literal.intValue(12))));

    Assert.assertTrue(function.canUseStarTree(Map.of()));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.HLLPLUS_ULL_P_KEY, "12")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.HLLPLUS_ULL_P_KEY, 12)));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.HLLPLUS_ULL_P_KEY, "16")));
  }

  @Test
  public void testCanUseStarTreeCustomP() {
    DistinctCountULLAggregationFunction function = new DistinctCountULLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"), ExpressionContext.forLiteral(Literal.stringValue("16"))));

    Assert.assertFalse(function.canUseStarTree(Map.of()));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.HLLPLUS_ULL_P_KEY, "12")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.HLLPLUS_ULL_P_KEY, 16)));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.HLLPLUS_ULL_P_KEY, "16")));
  }

  @Test
  public void testRawUuidUsesStoredBytes() {
    DistinctCountULLAggregationFunction function =
        new DistinctCountULLAggregationFunction(List.of(INPUT_EXPRESSION));
    byte[][] uuidValues = {UUID_0, UUID_1, UUID_0, UUID_2};
    BlockValSet blockValSet = mock(BlockValSet.class);
    when(blockValSet.getValueType()).thenReturn(DataType.UUID);
    when(blockValSet.isDictionaryEncoded()).thenReturn(false);
    when(blockValSet.getBytesValuesSV()).thenReturn(uuidValues);

    AggregationResultHolder aggregationResultHolder = function.createAggregationResultHolder();
    function.aggregate(uuidValues.length, aggregationResultHolder, Map.of(INPUT_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, aggregationResultHolder), 3L);

    GroupByResultHolder groupBySVResultHolder = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupBySV(uuidValues.length, new int[]{0, 0, 1, 1}, groupBySVResultHolder,
        Map.of(INPUT_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, groupBySVResultHolder, 0), 2L);
    Assert.assertEquals(extractFinalResult(function, groupBySVResultHolder, 1), 2L);

    GroupByResultHolder groupByMVResultHolder = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupByMV(uuidValues.length, new int[][]{{0}, {1}, {0, 1}, {1}}, groupByMVResultHolder,
        Map.of(INPUT_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, groupByMVResultHolder, 0), 1L);
    Assert.assertEquals(extractFinalResult(function, groupByMVResultHolder, 1), 3L);

    verify(blockValSet, atLeastOnce()).getBytesValuesSV();
    verify(blockValSet, never()).getBytesValuesMV();
  }

  @Test
  public void testSerializedBytesUsesStoredSketches() {
    DistinctCountULLAggregationFunction function =
        new DistinctCountULLAggregationFunction(List.of(INPUT_EXPRESSION));
    byte[][] serializedSketches = {
        serializedSketch(function, "a", "b"),
        serializedSketch(function, "b", "c"),
        serializedSketch(function, "a"),
        serializedSketch(function, "d")
    };
    BlockValSet blockValSet = mock(BlockValSet.class);
    when(blockValSet.getValueType()).thenReturn(DataType.BYTES);
    when(blockValSet.isDictionaryEncoded()).thenReturn(false);
    when(blockValSet.getBytesValuesSV()).thenReturn(serializedSketches);

    AggregationResultHolder aggregationResultHolder = function.createAggregationResultHolder();
    function.aggregate(serializedSketches.length, aggregationResultHolder, Map.of(INPUT_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, aggregationResultHolder),
        referenceEstimate(function, "a", "b", "c", "d"));

    GroupByResultHolder groupBySVResultHolder = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupBySV(serializedSketches.length, new int[]{0, 0, 1, 1}, groupBySVResultHolder,
        Map.of(INPUT_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, groupBySVResultHolder, 0),
        referenceEstimate(function, "a", "b", "c"));
    Assert.assertEquals(extractFinalResult(function, groupBySVResultHolder, 1), referenceEstimate(function, "a", "d"));

    GroupByResultHolder groupByMVResultHolder = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupByMV(serializedSketches.length, new int[][]{{0}, {1}, {0, 1}, {1}},
        groupByMVResultHolder, Map.of(INPUT_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, groupByMVResultHolder, 0), referenceEstimate(function, "a", "b"));
    Assert.assertEquals(extractFinalResult(function, groupByMVResultHolder, 1),
        referenceEstimate(function, "a", "b", "c", "d"));

    verify(blockValSet, atLeastOnce()).getBytesValuesSV();
    verify(blockValSet, never()).getBytesValuesMV();
  }

  private static byte[] serializedSketch(DistinctCountULLAggregationFunction function, String... values) {
    UltraLogLog sketch = UltraLogLog.create(function.getP());
    for (String value : values) {
      UltraLogLogUtils.hashObject(value).ifPresent(sketch::add);
    }
    return ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.serialize(sketch);
  }

  private static long referenceEstimate(DistinctCountULLAggregationFunction function, String... values) {
    UltraLogLog reference = UltraLogLog.create(function.getP());
    for (String value : values) {
      UltraLogLogUtils.hashObject(value).ifPresent(reference::add);
    }
    return Math.round(reference.getDistinctCountEstimate());
  }

  private static long extractFinalResult(DistinctCountULLAggregationFunction function,
      AggregationResultHolder resultHolder) {
    return ((Number) function.extractFinalResult(function.extractAggregationResult(resultHolder))).longValue();
  }

  private static long extractFinalResult(DistinctCountULLAggregationFunction function,
      GroupByResultHolder resultHolder, int groupKey) {
    return ((Number) function.extractFinalResult(function.extractGroupByResult(resultHolder, groupKey))).longValue();
  }
}
