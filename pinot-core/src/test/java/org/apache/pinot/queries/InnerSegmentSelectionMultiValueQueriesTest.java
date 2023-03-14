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
package org.apache.pinot.queries;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.query.EmptySelectionOperator;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@SuppressWarnings("ConstantConditions")
public class InnerSegmentSelectionMultiValueQueriesTest extends BaseMultiValueQueriesTest {
  private static final String SELECT_STAR_QUERY = "SELECT * FROM testTable";
  private static final String SELECTION_QUERY = "SELECT column1, column5, column6 FROM testTable";
  private static final String ORDER_BY = " ORDER BY column5, column9";

  @Test
  public void testSelectLimitZero() {
    String limit = " LIMIT 0";

    // Test query without filter
    EmptySelectionOperator emptySelectionOperator = getOperator(SELECT_STAR_QUERY + limit);
    SelectionResultsBlock resultsBlock = emptySelectionOperator.nextBlock();
    ExecutionStatistics executionStatistics = emptySelectionOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 0L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 0L);
    assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(selectionDataSchema.size(), 10);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column6"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), DataSchema.ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
    assertTrue(resultsBlock.getRows().isEmpty());

    // Test query with filter
    emptySelectionOperator = getOperator(SELECT_STAR_QUERY + FILTER + limit);
    resultsBlock = emptySelectionOperator.nextBlock();
    executionStatistics = emptySelectionOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 0L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 0L);
    assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);
    assertEquals(selectionDataSchema.size(), 10);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column6"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), DataSchema.ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
    assertTrue(resultsBlock.getRows().isEmpty());
  }

  @Test
  public void testSelectStar() {
    // Test query without filter
    BaseOperator<SelectionResultsBlock> selectionOnlyOperator = getOperator(SELECT_STAR_QUERY);
    SelectionResultsBlock resultsBlock = selectionOnlyOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 100L);
    assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(selectionDataSchema.size(), 10);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column6"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), DataSchema.ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
    List<Object[]> selectionResult = (List<Object[]>) resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] firstRow = selectionResult.get(0);
    assertEquals(firstRow.length, 10);
    assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 890282370);
    assertEquals(firstRow[columnIndexMap.get("column6")], new int[]{2147483647});

    // Test query with filter
    selectionOnlyOperator = getOperator(SELECT_STAR_QUERY + FILTER);
    resultsBlock = selectionOnlyOperator.nextBlock();
    executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 76044L);
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 100L);
    assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(selectionDataSchema.size(), 10);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column6"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), DataSchema.ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
    selectionResult = (List<Object[]>) resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    firstRow = selectionResult.get(0);
    assertEquals(firstRow.length, 10);
    assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 890282370);
    assertEquals(firstRow[columnIndexMap.get("column6")], new int[]{2147483647});
  }

  @Test
  public void testSelectionOnly() {
    // Test query without filter
    BaseOperator<SelectionResultsBlock> selectionOnlyOperator = getOperator(SELECTION_QUERY);
    SelectionResultsBlock resultsBlock = selectionOnlyOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 30L);
    assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(selectionDataSchema.size(), 3);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column6"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), DataSchema.ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
    List<Object[]> selectionResult = (List<Object[]>) resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] firstRow = selectionResult.get(0);
    assertEquals(firstRow.length, 3);
    assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 890282370);
    assertEquals(firstRow[columnIndexMap.get("column6")], new int[]{2147483647});

    // Test query with filter
    selectionOnlyOperator = getOperator(SELECTION_QUERY + FILTER);
    resultsBlock = selectionOnlyOperator.nextBlock();
    executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 76044L);
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 30L);
    assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(selectionDataSchema.size(), 3);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column6"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), DataSchema.ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
    selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    firstRow = selectionResult.get(0);
    assertEquals(firstRow.length, 3);
    assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 890282370);
    assertEquals(firstRow[columnIndexMap.get("column6")], new int[]{2147483647});
  }

  @Test
  public void testSelectionOrderBy() {
    // Test query without filter
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(SELECTION_QUERY + ORDER_BY);
    SelectionResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 100000L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 100000 * (2 order-by columns) + 10 * (2 non-order-by columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 200020L);
    assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(selectionDataSchema.size(), 4);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column6"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), DataSchema.ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
    List<Object[]> selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 4);
    assertEquals((String) lastRow[columnIndexMap.get("column5")], "AKXcXcIqsqOJFsdwxZ");
    assertEquals(lastRow[columnIndexMap.get("column6")], new int[]{1252});

    // Test query with filter
    selectionOrderByOperator = getOperator(SELECTION_QUERY + FILTER + ORDER_BY);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 15620L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 113888L);
    // 15620 * (2 order-by columns) + 10 * (2 non-order-by columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 31260L);
    assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(selectionDataSchema.size(), 4);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column6"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), DataSchema.ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
    selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 4);
    assertEquals((String) lastRow[columnIndexMap.get("column5")], "AKXcXcIqsqOJFsdwxZ");
    assertEquals(lastRow[columnIndexMap.get("column6")], new int[]{2147483647});
  }

  private Map<String, Integer> computeColumnNameToIndexMap(DataSchema dataSchema) {
    Map<String, Integer> columnIndexMap = new HashMap<>();

    for (int i = 0; i < dataSchema.size(); i++) {
      columnIndexMap.put(dataSchema.getColumnName(i), i);
    }
    return columnIndexMap;
  }
}
