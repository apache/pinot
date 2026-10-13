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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AnyValueAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AvgAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.CountAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.SumAggregationFunction;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Exercises serialized composite keys across merging and filtered aggregation. Each test owns its executor and blocks.
public class MultistageGroupByExecutorTest {
  private static final DataSchema INPUT_SCHEMA = new DataSchema(new String[]{"weight", "count", "tv", "tag"},
      new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.LONG, ColumnDataType.INT, ColumnDataType.STRING});

  @DataProvider
  public Object[][] stateRoundTrips() {
    return new Object[][]{{false, AggType.FINAL}, {true, AggType.FINAL},
        {false, AggType.INTERMEDIATE}, {true, AggType.INTERMEDIATE}};
  }

  @Test(dataProvider = "stateRoundTrips")
  public void testIntermediateStateRoundTrip(boolean serialized, AggType mergeMode) {
    DataSchema inputSchema = new DataSchema(new String[]{"value", "key"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.STRING});
    // The exported layout differs from the original input: keys precede intermediate values.
    DataSchema intermediateSchema = new DataSchema(new String[]{"key", "sum", "avg", "any"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE, ColumnDataType.OBJECT,
            ColumnDataType.OBJECT});
    DataSchema resultSchema = new DataSchema(intermediateSchema.getColumnNames(),
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE,
            ColumnDataType.DOUBLE});
    AggregationFunction<?, ?>[] mergeFunctions = stateFunctions();
    MultistageGroupByExecutor merger = MultistageGroupByExecutor.forSpillMerge(new int[]{0}, mergeFunctions,
        new int[]{-1, -1, -1}, -1, mergeMode,
        mergeMode == AggType.FINAL ? resultSchema : intermediateSchema, Map.of(), PlanNode.NodeHint.EMPTY, 1);

    for (MseBlock.Data block : List.of(
        OperatorTestUtil.block(inputSchema, new Object[]{2.0, "a"}, new Object[]{10.0, "b"}),
        OperatorTestUtil.block(inputSchema, new Object[]{4.0, "a"}))) {
      AggregationFunction<?, ?>[] inputFunctions = stateFunctions();
      MultistageGroupByExecutor input = MultistageGroupByExecutor.forSpillInput(new int[]{1}, inputFunctions,
          new int[]{-1, -1, -1}, -1, AggType.LEAF, intermediateSchema, Map.of(), PlanNode.NodeHint.EMPTY, 1);
      input.processBlock(block);
      // Build the wire schema from a fresh, merge-only function, not an already-aggregated instance.
      ColumnDataType[] wireTypes = intermediateSchema.getColumnDataTypes().clone();
      for (int i = 0; i < mergeFunctions.length; i++) {
        wireTypes[i + 1] = mergeFunctions[i].getIntermediateResultColumnType();
      }
      MseBlock.Data exported =
          exportStates(input, new DataSchema(intermediateSchema.getColumnNames(), wireTypes), inputFunctions);
      merger.processSpillBlock(serialized ? exported.asSerialized() : exported);
    }
    if (mergeMode == AggType.INTERMEDIATE) {
      MultistageGroupByExecutor finalMerger = MultistageGroupByExecutor.forSpillMerge(new int[]{0}, stateFunctions(),
          new int[]{-1, -1, -1}, -1, AggType.FINAL, resultSchema, Map.of(), PlanNode.NodeHint.EMPTY, 1);
      MseBlock.Data exported = exportStates(merger, intermediateSchema, mergeFunctions);
      finalMerger.processSpillBlock(serialized ? exported.asSerialized() : exported);
      merger = finalMerger;
    }
    List<Object[]> rows = merger.getResult(10);
    rows.sort((left, right) -> ((String) left[0]).compareTo((String) right[0]));
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{"a", 6.0, 3.0, 2.0});
    assertEquals(rows.get(1), new Object[]{"b", 10.0, 10.0, 10.0});
  }

  @Test
  public void testArrayKeysRetainIdentityAcrossSerialization() {
    DataSchema inputSchema = new DataSchema(new String[]{"key"}, new ColumnDataType[]{ColumnDataType.STRING_ARRAY});
    DataSchema intermediateSchema = new DataSchema(new String[]{"key", "count"},
        new ColumnDataType[]{ColumnDataType.STRING_ARRAY, ColumnDataType.LONG});
    AggregationFunction<?, ?>[] functions =
        {new CountAggregationFunction(List.of(ExpressionContext.forIdentifier("*")), false)};
    MultistageGroupByExecutor input = MultistageGroupByExecutor.forSpillInput(new int[]{0}, functions,
        new int[]{-1}, -1, AggType.LEAF, intermediateSchema, Map.of(), PlanNode.NodeHint.EMPTY, 1);
    input.processBlock(OperatorTestUtil.block(inputSchema, new Object[]{new String[]{"a", "b"}},
        new Object[]{new String[]{"a", "b"}}));
    MultistageGroupByExecutor merger = MultistageGroupByExecutor.forSpillMerge(new int[]{0}, functions,
        new int[]{-1}, -1, AggType.FINAL, intermediateSchema, Map.of(), PlanNode.NodeHint.EMPTY, 1);
    MseBlock.Data exported = exportStates(input, intermediateSchema, functions);
    merger.processSpillBlock(exported);
    merger.processSpillBlock(exported.asSerialized());
    List<Object[]> rows = merger.getResult(10);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0)[0], new String[]{"a", "b"});
    assertEquals(rows.get(0)[1], 4L);
  }

  @DataProvider
  public Object[][] spillTriggers() {
    return new Object[][]{{1}, {100}};
  }

  @Test(dataProvider = "spillTriggers")
  public void testSpillTriggerHonorsGroupLimit(int trigger) {
    DataSchema schema = new DataSchema(new String[]{"key", "sum"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE});
    AggregationFunction<?, ?>[] functions =
        {new SumAggregationFunction(List.of(ExpressionContext.forIdentifier("$1")), true)};
    Map<String, String> options = Map.of(QueryOptionKey.NUM_GROUPS_LIMIT, "2");
    MultistageGroupByExecutor input = MultistageGroupByExecutor.forSpillInput(new int[]{0}, functions,
        new int[]{-1}, -1, AggType.LEAF, schema, options, PlanNode.NodeHint.EMPTY, trigger);
    assertFalse(input.shouldSpill());
    input.processBlock(OperatorTestUtil.block(schema, new Object[]{1, 2.0}));
    assertEquals(input.shouldSpill(), trigger == 1);
    input.processBlock(OperatorTestUtil.block(schema, new Object[]{2, 3.0}, new Object[]{3, 100.0}));
    assertTrue(input.shouldSpill());
    assertEquals(input.getNumGroups(), 2);

    MultistageGroupByExecutor merger = MultistageGroupByExecutor.forSpillMerge(new int[]{0}, functions,
        new int[]{-1}, -1, AggType.FINAL, schema, options, PlanNode.NodeHint.EMPTY, 1);
    merger.processSpillBlock(exportStates(input, schema, functions).asSerialized());
    merger.processSpillBlock(OperatorTestUtil.block(schema, new Object[]{1, 5.0}, new Object[]{3, 100.0}));
    List<Object[]> rows = merger.getResult(10);
    rows.sort((left, right) -> Integer.compare((int) left[0], (int) right[0]));
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{1, 7.0});
    assertEquals(rows.get(1), new Object[]{2, 3.0});
  }

  private static AggregationFunction<?, ?>[] stateFunctions() {
    List<ExpressionContext> arguments = List.of(ExpressionContext.forIdentifier("$0"));
    return new AggregationFunction<?, ?>[]{new SumAggregationFunction(arguments, true),
        new AvgAggregationFunction(arguments, true), new AnyValueAggregationFunction(arguments, true)};
  }

  private static MseBlock.Data exportStates(MultistageGroupByExecutor executor, DataSchema schema,
      AggregationFunction<?, ?>[] functions) {
    List<Object[]> rows = new ArrayList<>();
    executor.getIntermediateResultIterator().forEachRemaining(rows::add);
    return new RowHeapDataBlock(rows, schema, functions);
  }

  @DataProvider
  public Object[][] mergeModes() {
    return new Object[][]{{2, false}, {2, true}, {3, false}, {3, true}};
  }

  @Test(dataProvider = "mergeModes")
  public void testSerializedKeysAcrossBlocks(int numKeys, boolean leafReturnFinalResult) {
    MultistageGroupByExecutor executor = newExecutor(numKeys, leafReturnFinalResult, 100);
    MseBlock.Data first = OperatorTestUtil.block(INPUT_SCHEMA,
        new Object[]{1.5, 2L, 1000, "Aa"},
        new Object[]{null, 3L, 1000, "BB"},
        new Object[]{1.5, 5L, null, "Aa"},
        new Object[]{null, 7L, null, null},
        new Object[]{0.0, 11L, 1000, "Aa"},
        new Object[]{-0.0, 13L, 1000, "BB"},
        new Object[]{Double.NaN, 17L, 1000, "Aa"},
        new Object[]{Double.POSITIVE_INFINITY, 19L, 1000, null}).asSerialized();
    executor.processBlock(first);
    executor.processBlock(OperatorTestUtil.block(INPUT_SCHEMA).asSerialized());
    executor.processBlock(OperatorTestUtil.block(INPUT_SCHEMA,
        new Object[]{null, 23L, null, null},
        new Object[]{Double.NaN, 29L, 1000, "Aa"},
        new Object[]{1.5, 31L, 1000, "Aa"}).asSerialized());

    List<Object[]> expected = List.of(
        new Object[]{1000, 1.5, "Aa", 33L},
        new Object[]{1000, null, "BB", 3L},
        new Object[]{null, 1.5, "Aa", 5L},
        new Object[]{null, null, null, 30L},
        new Object[]{1000, 0.0, "Aa", 11L},
        new Object[]{1000, -0.0, "BB", 13L},
        new Object[]{1000, Double.NaN, "Aa", 46L},
        new Object[]{1000, Double.POSITIVE_INFINITY, null, 19L});
    assertEquals(asMap(executor.getResult(100), numKeys), expectedMap(expected, numKeys));
    assertEquals(executor.getNumGroups(), 8);
  }

  @Test(dataProvider = "mergeModes")
  public void testExistingKeysStillMergeAtGroupLimit(int numKeys, boolean leafReturnFinalResult) {
    MultistageGroupByExecutor executor = newExecutor(numKeys, leafReturnFinalResult, 2);
    executor.processBlock(OperatorTestUtil.block(INPUT_SCHEMA,
        new Object[]{1.5, 2L, 1000, "Aa"},
        new Object[]{null, 3L, null, null},
        new Object[]{2.5, 100L, 2000, "BB"},
        new Object[]{1.5, 5L, 1000, "Aa"},
        new Object[]{null, 7L, null, null}).asSerialized());
    executor.processBlock(OperatorTestUtil.block(INPUT_SCHEMA,
        new Object[]{1.5, 100L, null, "Aa"},
        new Object[]{1.5, 11L, 1000, "Aa"}).asSerialized());

    assertEquals(asMap(executor.getResult(100), numKeys), expectedMap(List.of(
        new Object[]{1000, 1.5, "Aa", 18L}, new Object[]{null, null, null, 10L}), numKeys));
    assertTrue(executor.isNumGroupsLimitReached());
  }

  @DataProvider
  public Object[][] filteredModes() {
    return new Object[][]{{2, false}, {2, true}, {3, false}, {3, true}};
  }

  @Test(dataProvider = "filteredModes")
  public void testFilteredSerializedKeysAcrossBlocks(int numKeys, boolean skipEmptyGroups) {
    DataSchema inputSchema = new DataSchema(new String[]{"weight", "count", "tv", "tag", "filter"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.LONG, ColumnDataType.INT, ColumnDataType.STRING,
            ColumnDataType.BOOLEAN});
    MultistageGroupByExecutor executor = newExecutor(numKeys, false, 100, 4, skipEmptyGroups);
    executor.processBlock(OperatorTestUtil.block(inputSchema,
        new Object[]{1.5, 2L, 1000, "Aa", 1},
        new Object[]{9.5, 3L, 9000, "unmatched", 0},
        new Object[]{null, 5L, null, null, 1},
        new Object[]{1.5, 7L, 1000, "Aa", 0},
        new Object[]{2.5, 11L, 2000, "BB", 1}).asSerialized());
    executor.processBlock(OperatorTestUtil.block(inputSchema).asSerialized());
    executor.processBlock(OperatorTestUtil.block(inputSchema,
        new Object[]{9.5, 13L, 9000, "unmatched", 0}).asSerialized());
    executor.processBlock(OperatorTestUtil.block(inputSchema,
        new Object[]{null, 17L, null, null, 1},
        new Object[]{2.5, 19L, 2000, "BB", 0},
        new Object[]{1.5, 23L, 1000, "Aa", 1}).asSerialized());

    Map<List<Object>, Long> expected = expectedMap(List.of(
        new Object[]{1000, 1.5, "Aa", 2L},
        new Object[]{null, null, null, 2L},
        new Object[]{2000, 2.5, "BB", 1L}), numKeys);
    if (!skipEmptyGroups) {
      expected.put(Arrays.asList(Arrays.copyOf(new Object[]{9000, 9.5, "unmatched"}, numKeys)), 0L);
    }
    assertEquals(asMap(executor.getResult(100), numKeys), expected);
    assertEquals(executor.getNumGroups(), expected.size());
  }

  private static MultistageGroupByExecutor newExecutor(int numKeys, boolean leafReturnFinalResult, int groupLimit) {
    return newExecutor(numKeys, leafReturnFinalResult, groupLimit, -1, false);
  }

  private static MultistageGroupByExecutor newExecutor(int numKeys, boolean leafReturnFinalResult, int groupLimit,
      int filterArgId, boolean skipEmptyGroups) {
    int[] groupKeys = numKeys == 2 ? new int[]{2, 0} : new int[]{2, 0, 3};
    DataSchema resultSchema = numKeys == 2
        ? new DataSchema(new String[]{"tv", "weight", "count"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.LONG})
        : new DataSchema(new String[]{"tv", "weight", "tag", "count"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.STRING,
                ColumnDataType.LONG});
    AggregationFunction<?, ?>[] functions = {
        new CountAggregationFunction(List.of(ExpressionContext.forIdentifier("$1")), true)};
    return new MultistageGroupByExecutor(groupKeys, functions, new int[]{filterArgId}, filterArgId,
        filterArgId < 0 ? AggType.FINAL : AggType.DIRECT, leafReturnFinalResult, resultSchema,
        Map.of(QueryOptionKey.NUM_GROUPS_LIMIT, Integer.toString(groupLimit),
            QueryOptionKey.FILTERED_AGGREGATIONS_SKIP_EMPTY_GROUPS, Boolean.toString(skipEmptyGroups)), null);
  }

  private static Map<List<Object>, Long> expectedMap(List<Object[]> rows, int numKeys) {
    Map<List<Object>, Long> result = new HashMap<>();
    for (Object[] row : rows) {
      result.put(Arrays.asList(Arrays.copyOf(row, numKeys)), (Long) row[3]);
    }
    return result;
  }

  private static Map<List<Object>, Long> asMap(List<Object[]> rows, int numKeys) {
    Map<List<Object>, Long> result = new HashMap<>();
    for (Object[] row : rows) {
      assertNull(result.put(Arrays.asList(Arrays.copyOf(row, numKeys)), (Long) row[numKeys]), "Duplicate output group");
    }
    return result;
  }
}
