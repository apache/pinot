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


/// Exercises serialized composite keys and merge state across blocks. Each test owns its executor and input blocks.
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

  private static MultistageGroupByExecutor newExecutor(int numKeys, boolean leafReturnFinalResult, int groupLimit) {
    int[] groupKeys = numKeys == 2 ? new int[]{2, 0} : new int[]{2, 0, 3};
    DataSchema resultSchema = numKeys == 2
        ? new DataSchema(new String[]{"tv", "weight", "count"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.LONG})
        : new DataSchema(new String[]{"tv", "weight", "tag", "count"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.DOUBLE, ColumnDataType.STRING,
                ColumnDataType.LONG});
    AggregationFunction<?, ?>[] functions = {
        new CountAggregationFunction(List.of(ExpressionContext.forIdentifier("$1")), true)};
    return new MultistageGroupByExecutor(groupKeys, functions, new int[]{-1}, -1, AggType.FINAL, leafReturnFinalResult,
        resultSchema, Map.of(QueryOptionKey.NUM_GROUPS_LIMIT, Integer.toString(groupLimit)), null);
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
