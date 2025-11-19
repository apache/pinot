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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.DistinctResultsBlock;
import org.apache.pinot.core.operator.combine.merger.DistinctResultsBlockMerger;
import org.apache.pinot.core.query.distinct.table.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class DistinctResultsBlockMergerTest {

  private static final DataSchema SCHEMA =
      new DataSchema(new String[]{"col"}, new ColumnDataType[]{ColumnDataType.INT});

  @Test
  public void shouldRespectMaxRowsAcrossSegments() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SET \"maxRowsInDistinct\"=1000; SELECT DISTINCT col FROM myTable");
    DistinctResultsBlockMerger merger = new DistinctResultsBlockMerger(queryContext);

    DistinctResultsBlock merged = new DistinctResultsBlock(fakeTable(0, 800), queryContext);
    merged.setNumDocsScanned(800);
    // First block under budget
    assertFalse(merger.isQuerySatisfied(merged));

    // Merge second block that pushes us over the server-level budget (1600 rows total)
    DistinctResultsBlock blockToMerge = new DistinctResultsBlock(fakeTable(800, 800), queryContext);
    blockToMerge.setNumDocsScanned(800);
    merger.mergeResultsBlocks(merged, blockToMerge);

    assertEquals(merged.getEarlyTerminationReason(), BaseResultsBlock.EarlyTerminationReason.DISTINCT_MAX_ROWS);
    assertTrue(merger.isQuerySatisfied(merged), "Combine should stop once server-level budget is reached");
    assertEquals(merged.getNumDocsScanned(), 1600);
    assertEquals(merged.getDistinctTable().size(), 1600);
  }

  @Test
  public void shouldTrackRowsWithoutChangeAcrossSegments() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SET \"numRowsWithoutChangeInDistinct\"=4; SELECT DISTINCT col FROM myTable");
    DistinctResultsBlockMerger merger = new DistinctResultsBlockMerger(queryContext);

    DistinctResultsBlock merged = new DistinctResultsBlock(fakeTable(0, 2), queryContext);
    merged.setNumDocsScanned(2);

    DistinctResultsBlock blockToMerge = new DistinctResultsBlock(fakeTable(0, 2), queryContext);
    blockToMerge.setNumDocsScanned(4);

    merger.mergeResultsBlocks(merged, blockToMerge);

    assertEquals(merged.getEarlyTerminationReason(), BaseResultsBlock.EarlyTerminationReason.DISTINCT_NO_NEW_VALUES);
    assertTrue(merger.isQuerySatisfied(merged), "Combine should stop once no-change budget is reached");
  }

  @Test
  public void shouldStopOnTimeLimitAcrossSegments() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT DISTINCT col FROM myTable");

    {
      DistinctResultsBlockMerger merger = new DistinctResultsBlockMerger(queryContext);
      DistinctResultsBlock timedOut = new DistinctResultsBlock(fakeTable(0, 10), queryContext);
      timedOut.setEarlyTerminationReason(BaseResultsBlock.EarlyTerminationReason.DISTINCT_TIME_LIMIT);
      assertTrue(merger.isQuerySatisfied(timedOut), "Combine should stop when a segment hits time limit");
    }

    {
      DistinctResultsBlockMerger merger = new DistinctResultsBlockMerger(queryContext);
      DistinctResultsBlock merged = new DistinctResultsBlock(fakeTable(0, 10), queryContext);
      DistinctResultsBlock timedOut = new DistinctResultsBlock(fakeTable(10, 10), queryContext);
      timedOut.setEarlyTerminationReason(BaseResultsBlock.EarlyTerminationReason.DISTINCT_TIME_LIMIT);

      assertTrue(merger.isQuerySatisfied(timedOut));
      merger.mergeResultsBlocks(merged, timedOut);

      assertEquals(merged.getEarlyTerminationReason(), BaseResultsBlock.EarlyTerminationReason.DISTINCT_TIME_LIMIT);
      assertTrue(merger.isQuerySatisfied(merged), "Combine should stop after merging a timed-out segment");
    }
  }

  @Test
  public void shouldStopOnTimeLimitBudgetAcrossSegments() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SET \"maxExecutionTimeMsInDistinct\"=0; SELECT DISTINCT col FROM myTable");

    {
      DistinctResultsBlockMerger merger = new DistinctResultsBlockMerger(queryContext);
      DistinctResultsBlock block = new DistinctResultsBlock(fakeTable(0, 10), queryContext);
      assertTrue(merger.isQuerySatisfied(block));
      assertEquals(block.getEarlyTerminationReason(), BaseResultsBlock.EarlyTerminationReason.DISTINCT_TIME_LIMIT);
    }

    {
      DistinctResultsBlockMerger merger = new DistinctResultsBlockMerger(queryContext);
      DistinctResultsBlock merged = new DistinctResultsBlock(fakeTable(0, 10), queryContext);
      DistinctResultsBlock blockToMerge = new DistinctResultsBlock(fakeTable(10, 10), queryContext);
      merger.mergeResultsBlocks(merged, blockToMerge);
      assertTrue(merger.isQuerySatisfied(merged));
      assertEquals(merged.getEarlyTerminationReason(), BaseResultsBlock.EarlyTerminationReason.DISTINCT_TIME_LIMIT);
    }
  }

  private static DistinctTable fakeTable(int startInclusive, int count) {
    Set<Integer> values = new HashSet<>();
    for (int i = 0; i < count; i++) {
      values.add(startInclusive + i);
    }
    return new FakeDistinctTable(values);
  }

  /**
   * Minimal {@link DistinctTable} implementation backed by a set of integers.
   */
  private static class FakeDistinctTable extends DistinctTable {
    private final Set<Integer> _values;

    FakeDistinctTable(Set<Integer> values) {
      super(SCHEMA, Integer.MAX_VALUE, false);
      _values = values;
    }

    @Override
    public boolean hasOrderBy() {
      return false;
    }

    @Override
    public void mergeDistinctTable(DistinctTable distinctTable) {
      for (Object[] row : distinctTable.getRows()) {
        _values.add((Integer) row[0]);
      }
    }

    @Override
    public boolean mergeDataTable(DataTable dataTable) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
      return _values.size();
    }

    @Override
    public boolean isSatisfied() {
      return false;
    }

    @Override
    public List<Object[]> getRows() {
      List<Object[]> rows = new ArrayList<>(_values.size());
      for (Integer v : _values) {
        rows.add(new Object[]{v});
      }
      return rows;
    }

    @Override
    public DataTable toDataTable()
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResultTable toResultTable() {
      throw new UnsupportedOperationException();
    }
  }
}
