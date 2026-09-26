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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.CountAggregationFunction;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Exercises serialized composite keys across merging and filtered aggregation. Each test owns its executor and blocks.
public class MultistageGroupByExecutorTest {
  private static final DataSchema INPUT_SCHEMA = new DataSchema(new String[]{"weight", "count", "tv", "tag"},
      new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.LONG, ColumnDataType.INT, ColumnDataType.STRING});

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
        filterArgId < 0 ? AggType.FINAL : AggType.DIRECT, leafReturnFinalResult, INPUT_SCHEMA, resultSchema,
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
