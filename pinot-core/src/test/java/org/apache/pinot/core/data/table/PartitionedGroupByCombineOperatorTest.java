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
package org.apache.pinot.core.data.table;

// This test is in org.apache.pinot.core.data.table so it can access the
// package-private IntermediateRecord constructor for building synthetic segment results.

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.combine.PartitionedGroupByCombineOperator;
import org.apache.pinot.core.plan.ExplainInfo;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link PartitionedGroupByCombineOperator}.
 *
 * <p>Uses synthetic segment operators (each returning a single {@link IntermediateRecord}) to test the
 * end-to-end combine behavior: correct top-K result selection, and correct handling of the global trim path.
 */
public class PartitionedGroupByCombineOperatorTest {

  private static final DataSchema DATA_SCHEMA = new DataSchema(
      new String[]{"a", "sum(b)"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});

  /**
   * Verifies that the operator returns at most {@code trimSize} results and that the actual top groups
   * (those with the highest {@code SUM(b)} values) survive both the global-threshold trim and the final merge.
   *
   * <p>Setup: 500 groups, ORDER BY SUM(b) DESC LIMIT 5, trimSize=50, threshold=400.
   * A single worker thread processes all 500 groups, so the global trim fires once at group 400,
   * trimming each partition to the top 50. Groups added after the trim are retained. The final result
   * must contain the top 5 groups by value.
   */
  @Test
  public void testTopKSurvivesGlobalTrim()
      throws Exception {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT a, SUM(b) FROM t GROUP BY a ORDER BY SUM(b) DESC LIMIT 5");
    queryContext.setMinServerGroupTrimSize(50);
    queryContext.setGroupTrimThreshold(400);
    queryContext.setEndTimeMs(System.currentTimeMillis() + 30_000);

    int numGroups = 500;
    List<Operator> operators = new ArrayList<>(numGroups);
    for (int i = 0; i < numGroups; i++) {
      operators.add(new SingleGroupOperator(DATA_SCHEMA, queryContext, "group_" + i, (double) i));
    }

    // Use a single-threaded executor so one worker processes all 500 groups sequentially.
    // This guarantees the thread-local total exceeds the threshold=400 and the global trim fires.
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      PartitionedGroupByCombineOperator combine =
          new PartitionedGroupByCombineOperator(operators, queryContext, executor, 2);
      BaseResultsBlock resultBlock = combine.nextBlock();

      assertFalse(resultBlock instanceof ExceptionResultsBlock, "Expected successful result, got exception");
      Table table = ((GroupByResultsBlock) resultBlock).getTable();
      assertNotNull(table, "Result table must not be null");
      // After finish(), the table holds at most trimSize=50 records
      assertTrue(table.size() <= 50, "Result size must not exceed trimSize=50, got " + table.size());

      // The top 5 groups (group_499..group_495) must be present — if the global trim incorrectly
      // over-trimmed a hot partition before the global total reached 400, these groups could be lost.
      Set<String> expectedTopGroups = new HashSet<>();
      for (int i = numGroups - 5; i < numGroups; i++) {
        expectedTopGroups.add("group_" + i);
      }
      Set<String> actualGroups = new HashSet<>();
      table.iterator().forEachRemaining(r -> actualGroups.add((String) r.getValues()[0]));
      assertTrue(actualGroups.containsAll(expectedTopGroups),
          "Top 5 groups must be present in results. Expected: " + expectedTopGroups + ", Actual: " + actualGroups);
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Verifies correct combine behavior when there is no trim (total groups below the threshold).
   * All groups are returned up to trimSize, and the top groups are correct.
   */
  @Test
  public void testNoTrimWhenBelowThreshold()
      throws Exception {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT a, SUM(b) FROM t GROUP BY a ORDER BY SUM(b) DESC LIMIT 5");
    queryContext.setMinServerGroupTrimSize(50);
    queryContext.setGroupTrimThreshold(1_000_000);
    queryContext.setEndTimeMs(System.currentTimeMillis() + 30_000);

    int numGroups = 20;
    List<Operator> operators = new ArrayList<>(numGroups);
    for (int i = 0; i < numGroups; i++) {
      operators.add(new SingleGroupOperator(DATA_SCHEMA, queryContext, "group_" + i, (double) i));
    }

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      PartitionedGroupByCombineOperator combine =
          new PartitionedGroupByCombineOperator(operators, queryContext, executor, 2);
      BaseResultsBlock resultBlock = combine.nextBlock();

      assertFalse(resultBlock instanceof ExceptionResultsBlock, "Expected successful result, got exception");
      Table table = ((GroupByResultsBlock) resultBlock).getTable();
      assertNotNull(table);
      // All 20 groups fit within trimSize=50, so all should be present
      assertEquals(table.size(), numGroups, "All groups must be present when below trimSize");
    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Fake segment operator that returns a single group-by record.
   * Placed in {@code org.apache.pinot.core.data.table} to access the package-private
   * {@link IntermediateRecord} constructor.
   */
  @SuppressWarnings("rawtypes")
  private static class SingleGroupOperator implements Operator<GroupByResultsBlock> {
    private final DataSchema _dataSchema;
    private final QueryContext _queryContext;
    private final String _groupKey;
    private final double _value;

    SingleGroupOperator(DataSchema dataSchema, QueryContext queryContext, String groupKey, double value) {
      _dataSchema = dataSchema;
      _queryContext = queryContext;
      _groupKey = groupKey;
      _value = value;
    }

    @Override
    public GroupByResultsBlock nextBlock() {
      Key key = new Key(new Object[]{_groupKey});
      Record record = new Record(new Object[]{_groupKey, _value});
      List<IntermediateRecord> records =
          Collections.singletonList(new IntermediateRecord(key, record, new Comparable[]{_value}));
      return new GroupByResultsBlock(_dataSchema, records, _queryContext);
    }

    @Override
    public List<? extends Operator> getChildOperators() {
      return Collections.emptyList();
    }

    @Nullable
    @Override
    public String toExplainString() {
      return "SingleGroupOperator";
    }

    @Override
    public ExplainInfo getExplainInfo() {
      return null;
    }

    @Override
    public ExecutionStatistics getExecutionStatistics() {
      return new ExecutionStatistics(0, 0, 0, 0);
    }
  }
}
