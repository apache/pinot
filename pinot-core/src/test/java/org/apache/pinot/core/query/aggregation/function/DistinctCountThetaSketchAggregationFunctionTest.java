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
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.Constants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class DistinctCountThetaSketchAggregationFunctionTest {

  @Test
  public void testCanUseStarTreeDefaultK() {
    // Default aggregation function lgK = 12 / K=4096
    DistinctCountThetaSketchAggregationFunction function =
        new DistinctCountThetaSketchAggregationFunction(List.of(ExpressionContext.forIdentifier("col")), false);

    assertTrue(function.canUseStarTree(Map.of()));
    assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "4096")));
    assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, 4096)));
    assertFalse(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, 2048)));
  }

  @Test
  public void testCanUseCustomK() {
    DistinctCountThetaSketchAggregationFunction function = new DistinctCountThetaSketchAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"),
            ExpressionContext.forLiteral(Literal.stringValue("nominalEntries=32768"))), false);

    // Default StarTree lgK = 14 / K=16384
    assertFalse(function.canUseStarTree(Map.of()));
    assertFalse(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "16384")));
    assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "65536")));
    assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, 32768)));
    assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "32768")));
  }

  private static final ExpressionContext VALUE_COLUMN = ExpressionContext.forIdentifier("value");
  private static final ExpressionContext FILTER_COLUMN = ExpressionContext.forIdentifier("dim");
  // Row 0 goes to group 0, rows 1 and 2 to group 1
  private static final int[][] GROUP_KEYS = {{0}, {1}, {1}};

  /// Builds `DISTINCTCOUNTTHETASKETCH(value, params, 'dim = 1', '$1')`, whose only accumulator is the filtered one.
  private static DistinctCountThetaSketchAggregationFunction filtered() {
    return new DistinctCountThetaSketchAggregationFunction(
        List.of(VALUE_COLUMN, ExpressionContext.forLiteral(Literal.stringValue("nominalEntries=4096")),
            ExpressionContext.forLiteral(Literal.stringValue("dim = 1")),
            ExpressionContext.forLiteral(Literal.stringValue("$1"))), false);
  }

  /// Every row passes the predicate, so the filter never decides which rows count - only which group they land in.
  private static Map<ExpressionContext, BlockValSet> block(BlockValSet values) {
    return Map.of(VALUE_COLUMN, values, FILTER_COLUMN, SyntheticBlockValSets.Int.create(null, new int[]{1, 1, 1}));
  }

  private static long groupResult(DistinctCountThetaSketchAggregationFunction function, GroupByResultHolder holder,
      int groupKey) {
    return ((Number) function.extractFinalResult(function.extractGroupByResult(holder, groupKey))).longValue();
  }

  /// A filtered group-by over a multi-value key column credits each row to its own group keys.
  ///
  /// The group keys are read per row, so reading them at anything else collapses every matching row onto one row's
  /// groups.
  @Test
  public void testFilteredSVColumnGroupByMVCreditsEachRowToItsOwnGroups() {
    DistinctCountThetaSketchAggregationFunction function = filtered();
    GroupByResultHolder resultHolder = new ObjectGroupByResultHolder(2, 2);
    function.aggregateGroupByMV(3, GROUP_KEYS, resultHolder,
        block(SyntheticBlockValSets.Long.create(null, new long[]{10L, 20L, 30L})));

    assertEquals(groupResult(function, resultHolder, 0), 1L);
    assertEquals(groupResult(function, resultHolder, 1), 2L);
  }

  /// A filtered group-by over a multi-value column reads both the group keys and the values of the row it is on.
  @Test
  public void testFilteredMVColumnGroupByMVCreditsEachRowToItsOwnGroups() {
    DistinctCountThetaSketchAggregationFunction function = filtered();
    GroupByResultHolder resultHolder = new ObjectGroupByResultHolder(2, 2);
    function.aggregateGroupByMV(3, GROUP_KEYS, resultHolder,
        block(SyntheticBlockValSets.LongMV.create(null, new long[][]{{10L, 20L}, {30L}, {40L, 50L}})));

    assertEquals(groupResult(function, resultHolder, 0), 2L);
    assertEquals(groupResult(function, resultHolder, 1), 3L);
  }

  /// The string branch is a separate switch case from the numeric ones, with the same per-row reads.
  @Test
  public void testFilteredStringMVColumnGroupByMVCreditsEachRowToItsOwnGroups() {
    DistinctCountThetaSketchAggregationFunction function = filtered();
    GroupByResultHolder resultHolder = new ObjectGroupByResultHolder(2, 2);
    function.aggregateGroupByMV(3, GROUP_KEYS, resultHolder,
        block(SyntheticBlockValSets.StrMV.create(null, new String[][]{{"a", "b"}, {"c"}, {"d", "e"}})));

    assertEquals(groupResult(function, resultHolder, 0), 2L);
    assertEquals(groupResult(function, resultHolder, 1), 3L);
  }
}
