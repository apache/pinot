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
import java.util.PriorityQueue;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.EmptySelectionOperator;
import org.testng.Assert;
import org.testng.annotations.Test;


@SuppressWarnings("ConstantConditions")
public class InnerSegmentSelectionSingleValueQueriesTest extends BaseSingleValueQueriesTest {
  private static final String SELECTION = " column1, column5, column11";
  private static final String ORDER_BY = " ORDER BY column6, column1";

  @Test
  public void testSelectLimitZero() {
    String query = "SELECT * FROM testTable LIMIT 0";

    // Test query without filter
    EmptySelectionOperator emptySelectionOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = emptySelectionOperator.nextBlock();
    ExecutionStatistics executionStatistics = emptySelectionOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);
    Assert.assertEquals(selectionDataSchema.size(), 11);
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertTrue(columnIndexMap.containsKey("column11"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column11")),
        DataSchema.ColumnDataType.STRING);
    Assert.assertTrue(resultsBlock.getSelectionResult().isEmpty());

    // Test query with filter
    emptySelectionOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = emptySelectionOperator.nextBlock();
    executionStatistics = emptySelectionOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    Assert.assertEquals(selectionDataSchema.size(), 11);
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertTrue(columnIndexMap.containsKey("column11"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column11")),
        DataSchema.ColumnDataType.STRING);
    Assert.assertTrue(resultsBlock.getSelectionResult().isEmpty());
  }

  @Test
  public void testSelectStar() {
    String query = "SELECT * FROM testTable";

    // Test query without filter
    BaseOperator<IntermediateResultsBlock> selectionOnlyOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = selectionOnlyOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 110L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);
    Assert.assertEquals(selectionDataSchema.size(), 11);
    Assert.assertEquals(getVirtualColumns(selectionDataSchema), 0);
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertTrue(columnIndexMap.containsKey("column11"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column11")),
        DataSchema.ColumnDataType.STRING);
    List<Object[]> selectionResult = (List<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    Object[] firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 11);
    Assert.assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 1578964907);
    Assert.assertEquals((String) firstRow[columnIndexMap.get("column11")], "P");

    // Test query with filter
    selectionOnlyOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = selectionOnlyOperator.nextBlock();
    executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 44257L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 110L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);
    Assert.assertEquals(selectionDataSchema.size(), 11);
    Assert.assertEquals(getVirtualColumns(selectionDataSchema), 0);
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertTrue(columnIndexMap.containsKey("column11"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column11")),
        DataSchema.ColumnDataType.STRING);
    selectionResult = (List<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 11);
    Assert.assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 351823652);
    Assert.assertEquals((String) firstRow[columnIndexMap.get("column11")], "t");
  }

  @Test
  public void testSelectionOnly() {
    String query = "SELECT" + SELECTION + " FROM testTable";

    // Test query without filter
    BaseOperator<IntermediateResultsBlock> selectionOnlyOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = selectionOnlyOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 30L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    Assert.assertEquals(selectionDataSchema.size(), 3);
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertTrue(columnIndexMap.containsKey("column11"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column11")),
        DataSchema.ColumnDataType.STRING);
    List<Object[]> selectionResult = (List<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    Object[] firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 3);
    Assert.assertEquals(((Integer) firstRow[0]).intValue(), 1578964907);
    Assert.assertEquals((String) firstRow[2], "P");

    // Test query with filter
    selectionOnlyOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = selectionOnlyOperator.nextBlock();
    executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 44257L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 30L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);
    Assert.assertEquals(selectionDataSchema.size(), 3);
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertTrue(columnIndexMap.containsKey("column11"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column11")),
        DataSchema.ColumnDataType.STRING);
    selectionResult = (List<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 3);
    Assert.assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 351823652);
    Assert.assertEquals((String) firstRow[columnIndexMap.get("column11")], "t");
  }

  @Test
  public void testSelectionOrderBy() {
    String query = "SELECT" + SELECTION + " FROM testTable" + ORDER_BY;

    // Test query without filter
    BaseOperator<IntermediateResultsBlock> selectionOrderByOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 30000 * (2 order-by columns + 1 docId column) + 10 * (2 non-order-by columns)
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 90020L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    Assert.assertEquals(selectionDataSchema.size(), 4);
    Assert.assertTrue(columnIndexMap.containsKey("column6"));
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    PriorityQueue<Object[]> selectionResult = (PriorityQueue<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    Object[] lastRow = selectionResult.peek();
    Assert.assertEquals(lastRow.length, 4);
    Assert.assertEquals(((Integer) lastRow[columnIndexMap.get("column6")]).intValue(), 6043515);
    Assert.assertEquals(((Integer) lastRow[columnIndexMap.get("column1")]).intValue(), 10542595);

    // Test query with filter
    selectionOrderByOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 6129L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 80150L);
    // 6129 * (2 order-by columns + 1 docId column) + 10 * (2 non-order-by columns)
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 18407L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    Assert.assertEquals(selectionDataSchema.size(), 4);
    Assert.assertTrue(columnIndexMap.containsKey("column6"));
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    selectionResult = (PriorityQueue<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    lastRow = selectionResult.peek();
    Assert.assertEquals(lastRow.length, 4);
    Assert.assertEquals(((Integer) lastRow[columnIndexMap.get("column6")]).intValue(), 6043515);
    Assert.assertEquals(((Integer) lastRow[columnIndexMap.get("column1")]).intValue(), 462769197);
  }

  @Test
  public void testSelectStarOrderBy() {
    String query = "SELECT * " + " FROM testTable" + ORDER_BY;

    // Test query without filter
    BaseOperator<IntermediateResultsBlock> selectionOrderByOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 30000 * (2 order-by columns + 1 docId column) + 10 * (9 non-order-by columns)
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 90090L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    Assert.assertEquals(getVirtualColumns(selectionDataSchema), 0);
    Assert.assertEquals(selectionDataSchema.size(), 11);
    Assert.assertTrue(columnIndexMap.containsKey("column6"));
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    PriorityQueue<Object[]> selectionResult = (PriorityQueue<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    Object[] lastRow = selectionResult.peek();
    Assert.assertEquals(lastRow.length, 11);
    Assert.assertEquals(((Integer) lastRow[columnIndexMap.get("column6")]).intValue(), 6043515);
    Assert.assertEquals(((Integer) lastRow[columnIndexMap.get("column1")]).intValue(), 10542595);

    // Test query with filter
    selectionOrderByOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 6129L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 80150L);
    // 6129 * (2 order-by columns + 1 docId column) + 10 * (9 non-order-by columns)
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 18477L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    Assert.assertEquals(getVirtualColumns(selectionDataSchema), 0);
    Assert.assertEquals(selectionDataSchema.size(), 11);
    Assert.assertTrue(columnIndexMap.containsKey("column6"));
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    selectionResult = (PriorityQueue<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    lastRow = selectionResult.peek();
    Assert.assertEquals(lastRow.length, 11);
    Assert.assertEquals(((Integer) lastRow[columnIndexMap.get("column6")]).intValue(), 6043515);
    Assert.assertEquals(((Integer) lastRow[columnIndexMap.get("column1")]).intValue(), 462769197);
  }

  @Test
  public void testSelectStarOrderByLargeOffsetLimit() {
    String query = "SELECT * " + " FROM testTable" + ORDER_BY + " LIMIT 5000, 7000";

    // Test query without filter
    BaseOperator<IntermediateResultsBlock> selectionOrderByOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 30000 * (2 order-by columns + 1 docId column) + 12000 * (9 non-order-by columns)
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 198000L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    Assert.assertEquals(getVirtualColumns(selectionDataSchema), 0);
    Assert.assertEquals(selectionDataSchema.size(), 11);
    Assert.assertTrue(columnIndexMap.containsKey("column6"));
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    PriorityQueue<Object[]> selectionResult = (PriorityQueue<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 12000);
    Object[] lastRow = selectionResult.peek();
    Assert.assertEquals(lastRow.length, 11);
    Assert.assertEquals((int) lastRow[columnIndexMap.get("column6")], 296467636);
    Assert.assertEquals((int) lastRow[columnIndexMap.get("column1")], 1715964282);

    // Test query with filter
    selectionOrderByOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 6129L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 80150L);
    // 6129 * (2 order-by columns + 1 docId column) + 6129 * (9 non-order-by columns)
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 73548L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    Assert.assertEquals(getVirtualColumns(selectionDataSchema), 0);
    Assert.assertEquals(selectionDataSchema.size(), 11);
    Assert.assertTrue(columnIndexMap.containsKey("column6"));
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    selectionResult = (PriorityQueue<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 6129);
    lastRow = selectionResult.peek();
    Assert.assertEquals(lastRow.length, 11);
    Assert.assertEquals((int) lastRow[columnIndexMap.get("column6")], 499968041);
    Assert.assertEquals((int) lastRow[columnIndexMap.get("column1")], 335520083);
  }

  private int getVirtualColumns(DataSchema selectionDataSchema) {
    int virtualCols = 0;
    for (int i = 0; i < selectionDataSchema.size(); ++i) {
      if (selectionDataSchema.getColumnName(i).startsWith("$")) {
        virtualCols++;
      }
    }
    return virtualCols;
  }

  private Map<String, Integer> computeColumnNameToIndexMap(DataSchema dataSchema) {
    Map<String, Integer> columnIndexMap = new HashMap<>();

    for (int i = 0; i < dataSchema.size(); i++) {
      columnIndexMap.put(dataSchema.getColumnName(i), i);
    }
    return columnIndexMap;
  }
}
