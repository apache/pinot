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
package org.apache.pinot.query.runtime.operator;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AvgAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.PercentileAggregationFunction;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.segment.local.customobject.AvgPair;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Checks that merging serialized custom object intermediate results straight from a data block gives the same result
/// as merging the deserialized values of a row heap block.
public class SerializedIntermediateResultMergeTest {
  private static final DataSchema INPUT_SCHEMA = new DataSchema(new String[]{"key", "percentile", "avg"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.OBJECT, ColumnDataType.OBJECT});
  private static final DataSchema GROUP_BY_RESULT_SCHEMA = new DataSchema(new String[]{"key", "percentile", "avg"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
  private static final DataSchema AGGREGATION_RESULT_SCHEMA = new DataSchema(new String[]{"percentile", "avg"},
      new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});

  @Test
  public void testGroupByMergeOfSerializedBlocksMatchesRowHeapBlocks() {
    MultistageGroupByExecutor serializedExecutor = newGroupByExecutor();
    MultistageGroupByExecutor rowHeapExecutor = newGroupByExecutor();
    for (List<Object[]> rows : inputBlocks()) {
      serializedExecutor.processBlock(newBlock(rows).asSerialized());
      rowHeapExecutor.processBlock(newBlock(copyStates(rows)));
    }

    List<Object[]> expected = rowHeapExecutor.getResult(100);
    List<Object[]> actual = serializedExecutor.getResult(100);
    assertEquals(actual.size(), 3);
    assertEquals(toMap(actual), toMap(expected));
  }

  @Test
  public void testAggregationMergeOfSerializedBlocksMatchesRowHeapBlocks() {
    MultistageAggregationExecutor serializedExecutor = newAggregationExecutor();
    MultistageAggregationExecutor rowHeapExecutor = newAggregationExecutor();
    for (List<Object[]> rows : inputBlocks()) {
      serializedExecutor.processBlock(newBlock(rows).asSerialized());
      rowHeapExecutor.processBlock(newBlock(copyStates(rows)));
    }

    Object[] expected = rowHeapExecutor.getResult().get(0);
    Object[] actual = serializedExecutor.getResult().get(0);
    assertTrue(actual[0] instanceof Double && actual[1] instanceof Double);
    assertEquals(actual, expected);
  }

  /// Three blocks. Group 1 and 2 span blocks, a null state is skipped, and group 3 only has an empty list.
  private static List<List<Object[]>> inputBlocks() {
    return List.of(
        List.of(
            new Object[]{1, values(5, 1, 9), new AvgPair(15, 3)},
            new Object[]{2, values(100), new AvgPair(100, 1)},
            new Object[]{1, null, null}),
        List.of(),
        List.of(
            new Object[]{2, values(-3, 7, 7, 42), new AvgPair(53, 4)},
            new Object[]{1, values(2), new AvgPair(2, 1)},
            new Object[]{3, values(), new AvgPair(0, 0)}));
  }

  private static DoubleArrayList values(double... values) {
    return new DoubleArrayList(values);
  }

  /// The executors merge in place, so each executor gets its own copy of the mutable states.
  private static List<Object[]> copyStates(List<Object[]> rows) {
    List<Object[]> copies = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      Object[] copy = row.clone();
      if (copy[1] != null) {
        copy[1] = new DoubleArrayList((DoubleArrayList) copy[1]);
      }
      copies.add(copy);
    }
    return copies;
  }

  private static RowHeapDataBlock newBlock(List<Object[]> rows) {
    return new RowHeapDataBlock(rows, INPUT_SCHEMA, newFunctions());
  }

  private static AggregationFunction[] newFunctions() {
    return new AggregationFunction[]{
        new PercentileAggregationFunction(ExpressionContext.forIdentifier("$1"), 50, true),
        new AvgAggregationFunction(List.of(ExpressionContext.forIdentifier("$2")), true)
    };
  }

  private static MultistageGroupByExecutor newGroupByExecutor() {
    return new MultistageGroupByExecutor(new int[]{0}, newFunctions(), new int[]{-1, -1}, -1, AggType.FINAL, false,
        GROUP_BY_RESULT_SCHEMA, Map.of(), null);
  }

  private static MultistageAggregationExecutor newAggregationExecutor() {
    return new MultistageAggregationExecutor(newFunctions(), new int[]{-1, -1}, -1, AggType.FINAL,
        AGGREGATION_RESULT_SCHEMA);
  }

  private static Map<Object, List<Object>> toMap(List<Object[]> rows) {
    Map<Object, List<Object>> result = new HashMap<>();
    for (Object[] row : rows) {
      result.put(row[0], Arrays.asList(row).subList(1, row.length));
    }
    return result;
  }
}
