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
package org.apache.pinot.core.operator.combine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// Tests that [GroupByCombineOperator] correctly merges a MIX of BASE-layout blocks (grouping-set base
/// aggregation: union columns only, no $groupingId) and FULL-layout blocks (per-row expansion, with $groupingId)
/// in the same query. Mixed layouts are legitimate because the base-vs-expansion choice is per segment: the
/// MV-column carve-out and the base-group cardinality gate are evaluated against each segment's own metadata.
public class MixedLayoutGroupByCombineOperatorTest {
  private final ExecutorService _executorService = Executors.newFixedThreadPool(4);

  /// `GROUP BY GROUPING SETS ((d1), ())` over `SUM(m1)`: union columns = [d1], sets = {d1} (ordinal 0) and the
  /// grand total (ordinal 1). Full layout: [d1, $groupingId, sum(m1)]; base layout: [d1, sum(m1)].
  private static QueryContext queryContext() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT d1, SUM(m1) FROM t GROUP BY GROUPING SETS ((d1), ())");
    queryContext.setEndTimeMs(System.currentTimeMillis() + 60_000);
    return queryContext;
  }

  private static GroupByResultsBlock baseLayoutBlock(QueryContext queryContext) {
    DataSchema baseSchema = new DataSchema(new String[]{"d1", "sum(m1)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE});
    List<IntermediateRecord> records = List.of(
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{"a"}), new Record(new Object[]{"a", 1.0})),
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{"b"}), new Record(new Object[]{"b", 2.0})));
    return new GroupByResultsBlock(baseSchema, records, queryContext);
  }

  private static GroupByResultsBlock fullLayoutBlock(QueryContext queryContext) {
    DataSchema fullSchema = new DataSchema(new String[]{"d1", "$groupingId", "sum(m1)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.DOUBLE});
    List<IntermediateRecord> records = List.of(
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{"a", 0}),
            new Record(new Object[]{"a", 0, 5.0})),
        IntermediateRecord.withoutOrderByValues(new Key(new Object[]{null, 1}),
            new Record(new Object[]{null, 1, 7.0})));
    return new GroupByResultsBlock(fullSchema, records, queryContext);
  }

  @SuppressWarnings("rawtypes")
  private static Operator mockOperator(GroupByResultsBlock block) {
    Operator operator = mock(Operator.class);
    when(operator.nextBlock()).thenReturn(block);
    when(operator.getExecutionStatistics()).thenReturn(new ExecutionStatistics(0, 0, 0, 0));
    return operator;
  }

  @Test
  public void testMixedBaseAndExpansionBlocksMergeCorrectly()
      throws Exception {
    QueryContext queryContext = queryContext();
    // Segment 1 chose base aggregation (base-layout block); segment 2 chose per-row expansion (full layout).
    List<Operator> operators =
        List.of(mockOperator(baseLayoutBlock(queryContext)), mockOperator(fullLayoutBlock(queryContext)));
    GroupByCombineOperator combineOperator =
        new GroupByCombineOperator(operators, queryContext, _executorService);

    Object block = combineOperator.nextBlock();
    if (!(block instanceof GroupByResultsBlock)) {
      throw new AssertionError("Combine returned " + block.getClass().getSimpleName() + ": "
          + ((org.apache.pinot.core.operator.blocks.results.BaseResultsBlock) block).getErrorMessages());
    }
    GroupByResultsBlock mergedBlock = (GroupByResultsBlock) block;
    IndexedTable table = (IndexedTable) mergedBlock.getTable();

    // Base block derives to: (a,0)=1, (b,0)=2, (null,1)=3. Full block merges in: (a,0)+=5, (null,1)+=7.
    Map<String, Double> result = new HashMap<>();
    Iterator<Record> iterator = table.iterator();
    while (iterator.hasNext()) {
      Object[] values = iterator.next().getValues();
      result.put(values[0] + "|" + values[1], ((Number) values[2]).doubleValue());
    }
    assertEquals(result.size(), 3);
    assertEquals(result.get("a|0"), 6.0);
    assertEquals(result.get("b|0"), 2.0);
    assertEquals(result.get("null|1"), 10.0);
  }

  @AfterClass
  public void tearDown() {
    _executorService.shutdownNow();
  }
}
