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
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// Multi-value column support for `SEGMENTPARTITIONEDDISTINCTCOUNT`.
///
/// This function used to read the single-value accessors unconditionally, so a multi-value column was not something
/// it could aggregate at all. It now branches on `BlockValSet.isSingleValue()` the way the HyperLogLog and bitmap
/// families already did. Its counts are exact, so the answers are asserted directly rather than as estimates.
public class SegmentPartitionedDistinctCountAggregationFunctionTest {
  private static final ExpressionContext COLUMN = ExpressionContext.forIdentifier("column");
  private static final long[][] MV_ROWS = {{1L, 2L}, {3L, 4L}, {5L, 6L}, {1L, 3L}};
  private static final long[] FLATTENED = {1L, 2L, 3L, 4L, 5L, 6L, 1L, 3L};

  private static SegmentPartitionedDistinctCountAggregationFunction create() {
    return create(false);
  }

  private static SegmentPartitionedDistinctCountAggregationFunction create(boolean nullHandlingEnabled) {
    return new SegmentPartitionedDistinctCountAggregationFunction(List.of(COLUMN), nullHandlingEnabled);
  }

  private static Map<ExpressionContext, BlockValSet> mvBlock() {
    return Map.of(COLUMN, SyntheticBlockValSets.LongMV.create(null, MV_ROWS));
  }

  /// Every value of every row is counted, with repeats collapsing.
  @Test
  public void testMVColumnCountsEveryValue() {
    SegmentPartitionedDistinctCountAggregationFunction function = create();
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(MV_ROWS.length, resultHolder, mvBlock());

    // 1, 2, 3, 4, 5 and 6, with 1 and 3 seen twice
    assertEquals(function.extractFinalResult(function.extractAggregationResult(resultHolder)).longValue(), 6L);
  }

  /// Every value of a row lands in that row's group.
  @Test
  public void testMVColumnGroupBySV() {
    SegmentPartitionedDistinctCountAggregationFunction function = create();
    GroupByResultHolder resultHolder = new ObjectGroupByResultHolder(2, 2);
    // Rows 0 and 3 to group 0, giving 1, 2, 1, 3; rows 1 and 2 to group 1, giving 3, 4, 5, 6
    function.aggregateGroupBySV(MV_ROWS.length, new int[]{0, 1, 1, 0}, resultHolder, mvBlock());

    assertEquals(function.extractFinalResult(function.extractGroupByResult(resultHolder, 0)).longValue(), 3L);
    assertEquals(function.extractFinalResult(function.extractGroupByResult(resultHolder, 1)).longValue(), 4L);
  }

  /// A row's values land in every group key that row carries.
  @Test
  public void testMVColumnGroupByMV() {
    SegmentPartitionedDistinctCountAggregationFunction function = create();
    GroupByResultHolder resultHolder = new ObjectGroupByResultHolder(2, 2);
    int[][] groupKeys = {{0, 1}, {0}, {1}, {0, 1}};
    function.aggregateGroupByMV(MV_ROWS.length, groupKeys, resultHolder, mvBlock());

    // Group 0 sees rows 0, 1 and 3, giving 1, 2, 3, 4; group 1 sees rows 0, 2 and 3, giving 1, 2, 5, 6, 3
    assertEquals(function.extractFinalResult(function.extractGroupByResult(resultHolder, 0)).longValue(), 4L);
    assertEquals(function.extractFinalResult(function.extractGroupByResult(resultHolder, 1)).longValue(), 5L);
  }

  /// A single-value column still takes the single-value path, so the split did not disturb it.
  @Test
  public void testSVColumnStillAggregates() {
    SegmentPartitionedDistinctCountAggregationFunction function = create();
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(FLATTENED.length, resultHolder,
        Map.of(COLUMN, SyntheticBlockValSets.Long.create(null, FLATTENED)));

    assertEquals(function.extractFinalResult(function.extractAggregationResult(resultHolder)).longValue(), 6L);
  }

  /// A dictionary-encoded multi-value column collects dictionary ids and resolves them against the dictionary only
  /// when the result is extracted, so it is a separate path from the raw multi-value column above.
  @Test
  public void testDictionaryEncodedMVColumn() {
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.get(anyInt())).thenAnswer(invocation -> (long) (int) invocation.getArgument(0) + 1);
    // Ids 0..5 stand for values 1..6, so these rows carry the same values as MV_ROWS
    int[][] dictIds = {{0, 1}, {2, 3}, {4, 5}, {0, 2}};

    SegmentPartitionedDistinctCountAggregationFunction function = create();
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(dictIds.length, resultHolder,
        Map.of(COLUMN, SyntheticBlockValSets.DictIdsMV.create(null, dictIds, dictionary, DataType.LONG)));

    assertEquals(function.extractFinalResult(function.extractAggregationResult(resultHolder)).longValue(), 6L);
  }

  /// With null handling enabled, a null row of a multi-value column contributes none of its values.
  @Test
  public void testMVColumnSkipsNullRowsWhenNullHandlingEnabled() {
    SegmentPartitionedDistinctCountAggregationFunction function = create(true);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(MV_ROWS.length, resultHolder,
        Map.of(COLUMN, SyntheticBlockValSets.LongMV.create(RoaringBitmap.bitmapOf(1, 3), MV_ROWS)));

    // Rows 1 and 3 are null, so only 1, 2, 5 and 6 are counted
    assertEquals(function.extractFinalResult(function.extractAggregationResult(resultHolder)).longValue(), 4L);
  }

  /// With null handling disabled the null rows are read as the column default and still counted, which is the
  /// answer this mode has always given.
  @Test
  public void testMVColumnCountsNullRowsWhenNullHandlingDisabled() {
    SegmentPartitionedDistinctCountAggregationFunction function = create(false);
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(MV_ROWS.length, resultHolder,
        Map.of(COLUMN, SyntheticBlockValSets.LongMV.create(RoaringBitmap.bitmapOf(1, 3), MV_ROWS)));

    assertEquals(function.extractFinalResult(function.extractAggregationResult(resultHolder)).longValue(), 6L);
  }
}
