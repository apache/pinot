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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class Sum0AggregationFunctionTest {
  private static final ExpressionContext COLUMN = ExpressionContext.forIdentifier("col");

  @Test
  public void testFactoryCreatesSum0() {
    for (String fnName : new String[]{"SUM0(col)", "$SUM0(col)"}) {
      for (boolean nullHandlingEnabled : new boolean[]{false, true}) {
        FunctionContext functionContext = RequestContextUtils.getExpression(fnName).getFunction();
        AggregationFunction<?, ?> function =
            AggregationFunctionFactory.getAggregationFunction(functionContext, nullHandlingEnabled);
        assertTrue(function instanceof Sum0AggregationFunction,
            "Expected Sum0AggregationFunction for " + fnName + ", got: " + function.getClass());
        assertEquals(function.getType(), AggregationFunctionType.SUM0);
      }
    }
  }

  @Test
  public void testEmptyInputReturnsZero() {
    for (boolean nullHandlingEnabled : new boolean[]{false, true}) {
      Sum0AggregationFunction function = new Sum0AggregationFunction(List.of(COLUMN), nullHandlingEnabled);

      AggregationResultHolder aggHolder = function.createAggregationResultHolder();
      assertEquals(function.extractAggregationResult(aggHolder), 0.0);
      assertEquals(function.extractFinalResult(null), 0.0);
      assertEquals(function.extractFinalResult(function.extractAggregationResult(aggHolder)), 0.0);

      GroupByResultHolder groupByHolder = function.createGroupByResultHolder(4, 4);
      groupByHolder.ensureCapacity(2);
      assertEquals(function.extractGroupByResult(groupByHolder, 0), 0.0);
      assertEquals(function.extractGroupByResult(groupByHolder, 1), 0.0);
    }
  }

  @Test
  public void testAllNullBlockSkipsNullsWithNonZeroDefault() {
    int numDocs = 5;
    RoaringBitmap allNull = new RoaringBitmap();
    allNull.add(0L, numDocs);
    BlockValSet blockValSet = SyntheticBlockValSets.Int.create(numDocs, allNull, () -> 999);

    Sum0AggregationFunction nullAware = new Sum0AggregationFunction(List.of(COLUMN), true);
    AggregationResultHolder nullAwareHolder = nullAware.createAggregationResultHolder();
    nullAware.aggregate(numDocs, nullAwareHolder, Map.of(COLUMN, blockValSet));
    assertEquals(nullAware.extractAggregationResult(nullAwareHolder), 0.0);
    assertEquals(nullAware.extractFinalResult(nullAware.extractAggregationResult(nullAwareHolder)), 0.0);

    Sum0AggregationFunction nullUnaware = new Sum0AggregationFunction(List.of(COLUMN), false);
    AggregationResultHolder nullUnawareHolder = nullUnaware.createAggregationResultHolder();
    nullUnaware.aggregate(numDocs, nullUnawareHolder, Map.of(COLUMN, blockValSet));
    assertEquals(nullUnaware.extractAggregationResult(nullUnawareHolder), 5.0 * 999.0);
  }

  @Test
  public void testGroupBySvWithNullsAndEmptyGroups() {
    // 4 rows: values [10, 999 (null), 20, 999 (null)], groups [0, 0, 1, 2]
    // Group 0: 10 (null skipped), Group 1: 20, Group 2: all-null -> 0.0, Group 3: untouched -> 0.0
    double[] values = {10.0, 999.0, 20.0, 999.0};
    int[] groupKeys = {0, 0, 1, 2};
    RoaringBitmap nullBitmap = RoaringBitmap.bitmapOf(1, 3);
    int[] idx = {0};
    BlockValSet blockValSet = SyntheticBlockValSets.Double.create(values.length, nullBitmap, () -> values[idx[0]++]);

    Sum0AggregationFunction function = new Sum0AggregationFunction(List.of(COLUMN), true);
    GroupByResultHolder holder = function.createGroupByResultHolder(4, 4);
    holder.ensureCapacity(4);
    function.aggregateGroupBySV(values.length, groupKeys, holder, Map.of(COLUMN, blockValSet));

    assertEquals(function.extractGroupByResult(holder, 0), 10.0);
    assertEquals(function.extractGroupByResult(holder, 1), 20.0);
    assertEquals(function.extractGroupByResult(holder, 2), 0.0);
    assertEquals(function.extractGroupByResult(holder, 3), 0.0);
  }

  @Test
  public void testGroupByMvWithNulls() {
    double[][] mvValues = {{1.5, 2.5}, {100.0}, {3.0}};
    RoaringBitmap nullBitmap = RoaringBitmap.bitmapOf(1);
    BlockValSet blockValSet = SyntheticBlockValSets.DoubleMV.create(nullBitmap, mvValues);

    Sum0AggregationFunction function = new Sum0AggregationFunction(List.of(COLUMN), true);
    GroupByResultHolder holderSv = function.createGroupByResultHolder(3, 3);
    holderSv.ensureCapacity(3);
    function.aggregateGroupBySV(mvValues.length, new int[]{0, 1, 0}, holderSv, Map.of(COLUMN, blockValSet));
    assertEquals(function.extractGroupByResult(holderSv, 0), 7.0);
    assertEquals(function.extractGroupByResult(holderSv, 1), 0.0);
    assertEquals(function.extractGroupByResult(holderSv, 2), 0.0);

    GroupByResultHolder holderMv = function.createGroupByResultHolder(3, 3);
    holderMv.ensureCapacity(3);
    function.aggregateGroupByMV(mvValues.length, new int[][]{{0, 2}, {1, 2}, {0}}, holderMv,
        Map.of(COLUMN, blockValSet));
    assertEquals(function.extractGroupByResult(holderMv, 0), 7.0);
    assertEquals(function.extractGroupByResult(holderMv, 1), 0.0);
    assertEquals(function.extractGroupByResult(holderMv, 2), 4.0);
  }

  @Test
  public void testMergeWithNullAndZero() {
    Sum0AggregationFunction function = new Sum0AggregationFunction(List.of(COLUMN), true);
    assertEquals(AggregationFunctionUtils.merge(function, null, null), null);
    assertEquals(function.extractFinalResult(AggregationFunctionUtils.merge(function, null, null)), 0.0);
    assertEquals(AggregationFunctionUtils.merge(function, null, 5.0), 5.0);
    assertEquals(AggregationFunctionUtils.merge(function, 0.0, 5.0), 5.0);
    assertEquals(function.merge(12.5, 7.5), 20.0);
  }
}
