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
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.query.EmptySelectionOperator;
import org.apache.pinot.core.operator.query.SelectionPartiallyOrderedByAscOperator;
import org.apache.pinot.core.operator.query.SelectionPartiallyOrderedByDescOperation;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@SuppressWarnings("ConstantConditions")
public class InnerSegmentSelectionSingleValueQueriesTest extends BaseSingleValueQueriesTest {
  private static final String SELECT_STAR_QUERY = "SELECT * FROM testTable";
  private static final String SELECTION_QUERY = "SELECT column1, column5, column11 FROM testTable";
  private static final String ORDER_BY = " ORDER BY column6, column1";

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
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);
    assertEquals(selectionDataSchema.size(), 11);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column11"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column11")), ColumnDataType.STRING);
    assertTrue(resultsBlock.getRows().isEmpty());

    // Test query with filter
    emptySelectionOperator = getOperator(SELECT_STAR_QUERY + FILTER + limit);
    resultsBlock = emptySelectionOperator.nextBlock();
    executionStatistics = emptySelectionOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 0L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 0L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(selectionDataSchema.size(), 11);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column11"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column11")), ColumnDataType.STRING);
    assertTrue(resultsBlock.getRows().isEmpty());
  }

  @Test
  public void testSelectionOrderByAgoFunction() {
    String query = "SELECT daysSinceEpoch FROM testTable WHERE "
        + "dateTimeConvert(daysSinceEpoch, '1:DAYS:EPOCH', '1:MILLISECONDS:EPOCH', '1:MILLISECONDS') > ago('P1D') "
        + "ORDER BY daysSinceEpoch LIMIT 10";
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(query);
    SelectionResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    verifySelectionOrderByAgoFunctionResult(resultsBlock);

    query = "SELECT daysSinceEpoch from testTable WHERE fromEpochDays(daysSinceEpoch) > ago('P1D') "
        + "ORDER BY daysSinceEpoch LIMIT 10";
    selectionOrderByOperator = getOperator(query);
    resultsBlock = selectionOrderByOperator.nextBlock();
    verifySelectionOrderByAgoFunctionResult(resultsBlock);
  }

  private void verifySelectionOrderByAgoFunctionResult(SelectionResultsBlock resultsBlock) {
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);
    assertEquals(selectionDataSchema.size(), 1);
    assertEquals(getVirtualColumns(selectionDataSchema), 0);
    assertTrue(columnIndexMap.containsKey("daysSinceEpoch"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("daysSinceEpoch")), ColumnDataType.INT);

    List<Object[]> selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    for (Object[] row : selectionResult) {
      assertEquals(row.length, 1);
      assertEquals(((Integer) row[columnIndexMap.get("daysSinceEpoch")]).intValue(), 126164076);
    }
  }

  @Test
  public void testSelectStar() {
    // Test query without filter
    BaseOperator<SelectionResultsBlock> selectionOnlyOperator = getOperator(SELECT_STAR_QUERY);
    SelectionResultsBlock resultsBlock = selectionOnlyOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 110L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);
    assertEquals(selectionDataSchema.size(), 11);
    assertEquals(getVirtualColumns(selectionDataSchema), 0);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column11"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column11")), ColumnDataType.STRING);
    List<Object[]> selectionResult = (List<Object[]>) resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] firstRow = selectionResult.get(0);
    assertEquals(firstRow.length, 11);
    assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 1578964907);
    assertEquals((String) firstRow[columnIndexMap.get("column11")], "P");

    // Test query with filter
    selectionOnlyOperator = getOperator(SELECT_STAR_QUERY + FILTER);
    resultsBlock = selectionOnlyOperator.nextBlock();
    executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 48204L);
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 110L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);
    assertEquals(selectionDataSchema.size(), 11);
    assertEquals(getVirtualColumns(selectionDataSchema), 0);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column11"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column11")), ColumnDataType.STRING);
    selectionResult = (List<Object[]>) resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    firstRow = selectionResult.get(0);
    assertEquals(firstRow.length, 11);
    assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 351823652);
    assertEquals((String) firstRow[columnIndexMap.get("column11")], "t");
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
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(selectionDataSchema.size(), 3);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column11"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column11")), ColumnDataType.STRING);
    List<Object[]> selectionResult = (List<Object[]>) resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] firstRow = selectionResult.get(0);
    assertEquals(firstRow.length, 3);
    assertEquals(((Integer) firstRow[0]).intValue(), 1578964907);
    assertEquals((String) firstRow[2], "P");

    // Test query with filter
    selectionOnlyOperator = getOperator(SELECTION_QUERY + FILTER);
    resultsBlock = selectionOnlyOperator.nextBlock();
    executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 48204L);
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 30L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);
    assertEquals(selectionDataSchema.size(), 3);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column11"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column11")), ColumnDataType.STRING);
    selectionResult = (List<Object[]>) resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    firstRow = selectionResult.get(0);
    assertEquals(firstRow.length, 3);
    assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 351823652);
    assertEquals((String) firstRow[columnIndexMap.get("column11")], "t");
  }

  @Test
  public void testSelectionOrderBy() {
    // Test query without filter
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(SELECTION_QUERY + ORDER_BY);
    SelectionResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 30000 * (2 order-by columns) + 10 * (2 non-order-by columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 60020L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(selectionDataSchema.size(), 4);
    assertTrue(columnIndexMap.containsKey("column6"));
    assertTrue(columnIndexMap.containsKey("column1"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")), ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), ColumnDataType.INT);
    List<Object[]> selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 4);
    assertEquals(((Integer) lastRow[columnIndexMap.get("column6")]).intValue(), 6043515);
    assertEquals(((Integer) lastRow[columnIndexMap.get("column1")]).intValue(), 10542595);

    // Test query with filter
    selectionOrderByOperator = getOperator(SELECTION_QUERY + FILTER + ORDER_BY);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 6129L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 63064L);
    // 6129 * (2 order-by columns) + 10 * (2 non-order-by columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 12278L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(selectionDataSchema.size(), 4);
    assertTrue(columnIndexMap.containsKey("column6"));
    assertTrue(columnIndexMap.containsKey("column1"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")), ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), ColumnDataType.INT);
    selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 4);
    assertEquals(((Integer) lastRow[columnIndexMap.get("column6")]).intValue(), 6043515);
    assertEquals(((Integer) lastRow[columnIndexMap.get("column1")]).intValue(), 462769197);
  }

  @Test
  public void testSelectionOrderBySortedColumn() {
    // Test query order by single sorted column in ascending order
    String orderBy = " ORDER BY column5";
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(SELECTION_QUERY + orderBy);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByAscOperator);
    SelectionResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 10 * (3 columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 30L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column5", "column1", "column11"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});
    List<Object[]> selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 3);
    assertEquals(lastRow[0], "gFuH");

    // Test query order by single sorted column in descending order
    orderBy = " ORDER BY column5 DESC";
    selectionOrderByOperator = getOperator(SELECTION_QUERY + orderBy);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByDescOperation);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 30000 * (3 columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 90000L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column5", "column1", "column11"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});
    selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 3);
    assertEquals(lastRow[0], "gFuH");

    // Test query order by all sorted columns in ascending order
    String query = "SELECT column5, daysSinceEpoch FROM testTable ORDER BY column5, daysSinceEpoch";
    selectionOrderByOperator = getOperator(query);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByAscOperator);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 10 * (2 columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 20L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column5", "daysSinceEpoch"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 2);
    assertEquals(lastRow[0], "gFuH");
    assertEquals(lastRow[1], 126164076);

    // Test query order by all sorted columns in descending order
    query = "SELECT column5 FROM testTable ORDER BY column5 DESC, daysSinceEpoch DESC";
    selectionOrderByOperator = getOperator(query);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByDescOperation);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 30000 * (2 columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 60000L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column5", "daysSinceEpoch"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 2);
    assertEquals(lastRow[0], "gFuH");
    assertEquals(lastRow[1], 167572854);

    // Test query order by one sorted column in ascending order, the other sorted column in descending order
    query = "SELECT daysSinceEpoch FROM testTable ORDER BY column5, daysSinceEpoch DESC";
    selectionOrderByOperator = getOperator(query);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByAscOperator);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 30000 * (2 columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 60000L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column5", "daysSinceEpoch"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 2);
    assertEquals(lastRow[0], "gFuH");
    assertEquals(lastRow[1], 167572854);

    // Test query order by one sorted column in ascending order, and some unsorted columns
    query = "SELECT column1 FROM testTable ORDER BY column5, column6, column1";
    selectionOrderByOperator = getOperator(query);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByAscOperator);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 30000 * (3 columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 90000L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column5", "column6", "column1"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.INT});
    selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 3);
    assertEquals(lastRow[0], "gFuH");
    // Unsorted column values should be the same as ordering by their own
    assertEquals(lastRow[1], 6043515);
    assertEquals(lastRow[2], 10542595);

    // Test query order by one sorted column in descending order, and some unsorted columns
    query = "SELECT column6 FROM testTable ORDER BY column5 DESC, column6, column1";
    selectionOrderByOperator = getOperator(query);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByDescOperation);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 30000 * (3 columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 90000L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column5", "column6", "column1"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.INT});
    selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 3);
    assertEquals(lastRow[0], "gFuH");
    // Unsorted column values should be the same as ordering by their own
    assertEquals(lastRow[1], 6043515);
    assertEquals(lastRow[2], 10542595);
  }

  @Test
  public void testSelectStarOrderBy() {
    // Test query without filter
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(SELECT_STAR_QUERY + ORDER_BY);
    SelectionResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 30000 * (2 order-by columns) + 10 * (9 non-order-by columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 60090L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(getVirtualColumns(selectionDataSchema), 0);
    assertEquals(selectionDataSchema.size(), 11);
    assertTrue(columnIndexMap.containsKey("column6"));
    assertTrue(columnIndexMap.containsKey("column1"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")), ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), ColumnDataType.INT);
    List<Object[]> selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 11);
    assertEquals(((Integer) lastRow[columnIndexMap.get("column6")]).intValue(), 6043515);
    assertEquals(((Integer) lastRow[columnIndexMap.get("column1")]).intValue(), 10542595);

    // Test query with filter
    selectionOrderByOperator = getOperator(SELECT_STAR_QUERY + FILTER + ORDER_BY);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 6129L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 63064L);
    // 6129 * (2 order-by columns) + 10 * (9 non-order-by columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 12348L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(getVirtualColumns(selectionDataSchema), 0);
    assertEquals(selectionDataSchema.size(), 11);
    assertTrue(columnIndexMap.containsKey("column6"));
    assertTrue(columnIndexMap.containsKey("column1"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")), ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), ColumnDataType.INT);
    selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 11);
    assertEquals(((Integer) lastRow[columnIndexMap.get("column6")]).intValue(), 6043515);
    assertEquals(((Integer) lastRow[columnIndexMap.get("column1")]).intValue(), 462769197);
  }

  @Test
  public void testSelectStarOrderBySortedColumn() {
    String orderBy = " ORDER BY column5";

    // Test query without filter
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(SELECT_STAR_QUERY + orderBy);
    SelectionResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 10 * (11 columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 110L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(getVirtualColumns(selectionDataSchema), 0);
    assertEquals(selectionDataSchema.size(), 11);
    assertTrue(columnIndexMap.containsKey("column5"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column5")), ColumnDataType.STRING);
    List<Object[]> selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 11);
    assertEquals((lastRow[columnIndexMap.get("column5")]), "gFuH");

    // Test query with filter
    selectionOrderByOperator = getOperator(SELECT_STAR_QUERY + FILTER + orderBy);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 10);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 48204L);
    // 10 * (11 columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 110);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(getVirtualColumns(selectionDataSchema), 0);
    assertEquals(selectionDataSchema.size(), 11);
    assertTrue(columnIndexMap.containsKey("column5"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column5")), ColumnDataType.STRING);
    selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 11);
    assertEquals((lastRow[columnIndexMap.get("column5")]), "gFuH");
  }

  @Test
  public void testSelectStarOrderByLargeOffsetLimit() {
    String limit = " LIMIT 5000, 7000";

    // Test query without filter
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(SELECT_STAR_QUERY + ORDER_BY + limit);
    SelectionResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 30000 * (2 order-by columns) + 12000 * (9 non-order-by columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 168000L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(getVirtualColumns(selectionDataSchema), 0);
    assertEquals(selectionDataSchema.size(), 11);
    assertTrue(columnIndexMap.containsKey("column6"));
    assertTrue(columnIndexMap.containsKey("column1"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")), ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), ColumnDataType.INT);
    List<Object[]> selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 12000);
    Object[] lastRow = selectionResult.get(11999);
    assertEquals(lastRow.length, 11);
    assertEquals((int) lastRow[columnIndexMap.get("column6")], 296467636);
    assertEquals((int) lastRow[columnIndexMap.get("column1")], 1715964282);

    // Test query with filter
    selectionOrderByOperator = getOperator(SELECT_STAR_QUERY + FILTER + ORDER_BY + limit);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 6129L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 63064L);
    // 6129 * (2 order-by columns) + 6129 * (9 non-order-by columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 67419L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    assertEquals(getVirtualColumns(selectionDataSchema), 0);
    assertEquals(selectionDataSchema.size(), 11);
    assertTrue(columnIndexMap.containsKey("column6"));
    assertTrue(columnIndexMap.containsKey("column1"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")), ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), ColumnDataType.INT);
    selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 6129);
    lastRow = selectionResult.get(6128);
    assertEquals(lastRow.length, 11);
    assertEquals((int) lastRow[columnIndexMap.get("column6")], 499968041);
    assertEquals((int) lastRow[columnIndexMap.get("column1")], 335520083);
  }

  int getVirtualColumns(DataSchema selectionDataSchema) {
    int virtualCols = 0;
    for (int i = 0; i < selectionDataSchema.size(); i++) {
      if (selectionDataSchema.getColumnName(i).startsWith("$")) {
        virtualCols++;
      }
    }
    return virtualCols;
  }

  Map<String, Integer> computeColumnNameToIndexMap(DataSchema dataSchema) {
    Map<String, Integer> columnIndexMap = new HashMap<>();

    for (int i = 0; i < dataSchema.size(); i++) {
      columnIndexMap.put(dataSchema.getColumnName(i), i);
    }
    return columnIndexMap;
  }

  @Test
  public void testThreadCpuTime() {
    String query = "SELECT * FROM testTable";

    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    // NOTE: Need to check whether thread CPU time measurement is enabled because some environments might not support
    //       ThreadMXBean.getCurrentThreadCpuTime()
    if (ThreadResourceUsageProvider.isThreadCpuTimeMeasurementEnabled()) {
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      assertTrue(brokerResponse.getOfflineThreadCpuTimeNs() > 0);
      assertTrue(brokerResponse.getRealtimeThreadCpuTimeNs() > 0);
    }

    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(false);
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    assertEquals(brokerResponse.getOfflineThreadCpuTimeNs(), 0);
    assertEquals(brokerResponse.getRealtimeThreadCpuTimeNs(), 0);
  }
}
