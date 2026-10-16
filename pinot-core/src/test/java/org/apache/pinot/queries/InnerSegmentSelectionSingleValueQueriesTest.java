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
import org.apache.pinot.core.operator.query.SelectionPartiallyOrderedByDescOperation;
import org.apache.pinot.core.operator.query.SelectionPartiallyOrderedByLinearOperator;
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
  public void testSelectionOrderBySingleSortedColumnAsc() {
    String orderBy = " ORDER BY column5";
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(SELECTION_QUERY + orderBy);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByLinearOperator);
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
  }

  @Test
  public void testSelectionOrderBySingleSortedColumnDescDidOrder() {
    String orderBy = " ORDER BY column5 DESC";
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(
        "SET allowReverseOrder = false; SET lateMaterializeSortedOrderBy = true; " + SELECTION_QUERY + orderBy);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByDescOperation, "Expected: "
        + SelectionPartiallyOrderedByLinearOperator.class + ", found: " + selectionOrderByOperator.getClass());
    SelectionResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // The scan reads only the order-by column (30000 * 1); column1 and column11 are fetched just for the
    // 10 rows that survive the limit (10 * 2). Before late materialisation this was 30000 * 3.
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 30000L + 20L);
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
  }

  /// The optimization is opt-in, so the same query without the option must still read every selected column
  /// for the whole match set. Pins the default; delete or invert this when the default flips.
  @Test
  public void testSelectionOrderBySingleSortedColumnDescIsEagerByDefault() {
    BaseOperator<SelectionResultsBlock> operator =
        getOperator("SET allowReverseOrder = false; " + SELECTION_QUERY + " ORDER BY column5 DESC");
    operator.nextBlock();
    // 30000 docs * 3 selected columns, as before late materialisation existed.
    assertEquals(operator.getExecutionStatistics().getNumEntriesScannedPostFilter(), 90000L);
  }

  @Test
  public void testSelectionOrderBySingleSortedColumnDescReverseOrder() {
    String orderBy = " ORDER BY column5 DESC";
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(
        "SET allowReverseOrder = true; " + SELECTION_QUERY + orderBy);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByLinearOperator, "Expected: "
        + SelectionPartiallyOrderedByLinearOperator.class + ", found: " + selectionOrderByOperator.getClass());
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
  }

  @Test
  public void testSelectionOrderByAllSortedColumnsAsc() {
    String query = "SELECT column5, daysSinceEpoch FROM testTable ORDER BY column5, daysSinceEpoch";
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(query);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByLinearOperator);
    SelectionResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 10 * (2 columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 20L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column5", "daysSinceEpoch"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    List<Object[]> selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 2);
    assertEquals(lastRow[0], "gFuH");
    assertEquals(lastRow[1], 126164076);
  }

  @Test
  public void testSelectionOrderByAllSortedColumnsDescDidOrder() {
    String query = "SET allowReverseOrder = false;"
        + "SELECT column5 "
        + "FROM testTable "
        + "ORDER BY column5 DESC, daysSinceEpoch DESC";
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(query);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByDescOperation);
    SelectionResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 30000 * (2 columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 60000L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column5", "daysSinceEpoch"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    List<Object[]> selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 2);
    assertEquals(lastRow[0], "gFuH");
    assertEquals(lastRow[1], 167572854);
  }

  @Test
  public void testSelectionOrderByAllSortedColumnsDescReverseOrder() {
    String query = "SET allowReverseOrder = true;"
        + "SELECT column5 "
        + "FROM testTable "
        + "ORDER BY column5 DESC, daysSinceEpoch DESC";
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(query);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByLinearOperator);
    SelectionResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 10 * (2 columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 20L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column5", "daysSinceEpoch"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    List<Object[]> selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 2);
    assertEquals(lastRow[0], "gFuH");
    assertEquals(lastRow[1], 167572854);
  }

  @Test
  public void testSelectionOrderByMixedSortedColumns() {
    String query = "SELECT daysSinceEpoch FROM testTable ORDER BY column5, daysSinceEpoch DESC";
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(query);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByLinearOperator);
    SelectionResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 30000 * (2 columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 60000L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column5", "daysSinceEpoch"});
    assertEquals(dataSchema.getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    List<Object[]> selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 2);
    assertEquals(lastRow[0], "gFuH");
    assertEquals(lastRow[1], 167572854);
  }

  @Test
  public void testSelectionOrderBySortedAndUnsortedColumnsAsc() {
    String query = "SELECT column1 FROM testTable ORDER BY column5, column6, column1";
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(query);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByLinearOperator);
    SelectionResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 30000 * (3 columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 90000L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column5", "column6", "column1"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.INT});
    List<Object[]> selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 3);
    assertEquals(lastRow[0], "gFuH");
    assertEquals(lastRow[1], 6043515);
    assertEquals(lastRow[2], 10542595);
  }

  @Test
  public void testSelectionOrderBySortedAndUnsortedColumnsDescDidOrder() {
    String query = "SET allowReverseOrder = false; "
        + "SELECT  column6 FROM testTable ORDER BY column5 DESC, column6, column1";
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(query);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByDescOperation);
    SelectionResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 30000 * (3 columns)
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 90000L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column5", "column6", "column1"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.INT});
    List<Object[]> selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 3);
    assertEquals(lastRow[0], "gFuH");
    assertEquals(lastRow[1], 6043515);
    // Unsorted column values should be the same as ordering by their own
    assertEquals(lastRow[2], 10542595);
  }

  @Test
  public void testSelectionOrderBySortedAndUnsortedColumnsDescReverseOrder() {
    String query = "SET allowReverseOrder = true; "
        + "SELECT column6 FROM testTable ORDER BY column5 DESC, column6, column1";
    BaseOperator<SelectionResultsBlock> selectionOrderByOperator = getOperator(query);
    assertTrue(selectionOrderByOperator instanceof SelectionPartiallyOrderedByLinearOperator);
    SelectionResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 90000L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema dataSchema = resultsBlock.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"column5", "column6", "column1"});
    assertEquals(dataSchema.getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.INT});
    List<Object[]> selectionResult = resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] lastRow = selectionResult.get(9);
    assertEquals(lastRow.length, 3);
    assertEquals(lastRow[0], "gFuH");
    assertEquals(lastRow[1], 6043515);
    // Unsorted column values should be the same as ordering by their own
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

  @Test
  public void testThreadMemAllocatedBytes() {
    String query = "SELECT * FROM testTable";

    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    // NOTE: Need to check whether thread mem measurement is enabled because some environments might not support
    //       ThreadMXBean.getThreadAllocatedBytes()
    if (ThreadResourceUsageProvider.isThreadMemoryMeasurementEnabled()) {
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      assertTrue(brokerResponse.getOfflineThreadMemAllocatedBytes() > 0);
      assertTrue(brokerResponse.getRealtimeThreadMemAllocatedBytes() > 0);
    }

    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(false);
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    assertEquals(brokerResponse.getOfflineThreadMemAllocatedBytes(), 0);
    assertEquals(brokerResponse.getRealtimeThreadMemAllocatedBytes(), 0);
  }

  /// Runs `query` with late materialisation on and off and asserts the answers are identical. column5 is the sorted
  /// column in this fixture.
  ///
  /// @param expectFewerEntries whether late materialisation should read fewer entries post-filter. It should not when
  ///                           the scan already stops at the limit, which is the case for the linear operator with a
  ///                           fully sorted order-by prefix.
  private void assertSameAnswerWithAndWithoutLateMaterialization(String query, boolean expectFewerEntries) {
    BaseOperator<SelectionResultsBlock> lateOperator = getOperator("SET lateMaterializeSortedOrderBy = true; " + query);
    SelectionResultsBlock lateBlock = lateOperator.nextBlock();
    ExecutionStatistics lateStats = lateOperator.getExecutionStatistics();

    BaseOperator<SelectionResultsBlock> eagerOperator =
        getOperator("SET lateMaterializeSortedOrderBy = false; " + query);
    SelectionResultsBlock eagerBlock = eagerOperator.nextBlock();
    ExecutionStatistics eagerStats = eagerOperator.getExecutionStatistics();

    DataSchema lateSchema = lateBlock.getDataSchema();
    DataSchema eagerSchema = eagerBlock.getDataSchema();
    assertEquals(lateSchema.getColumnNames(), eagerSchema.getColumnNames());
    assertEquals(lateSchema.getColumnDataTypes(), eagerSchema.getColumnDataTypes());

    List<Object[]> lateRows = lateBlock.getRows();
    List<Object[]> eagerRows = eagerBlock.getRows();
    assertEquals(lateRows.size(), eagerRows.size());
    for (int i = 0; i < lateRows.size(); i++) {
      assertEquals(lateRows.get(i), eagerRows.get(i), "row " + i + " differs for query: " + query);
    }
    assertEquals(lateStats.getNumDocsScanned(), eagerStats.getNumDocsScanned());

    long late = lateStats.getNumEntriesScannedPostFilter();
    long eager = eagerStats.getNumEntriesScannedPostFilter();
    if (expectFewerEntries) {
      assertTrue(late < eager, "expected fewer entries post-filter, but read " + late + " vs " + eager
          + " for query: " + query);
    } else {
      assertEquals(late, eager, "expected the same entries post-filter for query: " + query);
    }
  }

  /// The linear operator stops at the limit, so there is nothing to defer and the plan keeps projecting everything.
  @Test
  public void testLateMaterializationSkippedWhenScanStopsAtLimit() {
    assertSameAnswerWithAndWithoutLateMaterialization(SELECTION_QUERY + " ORDER BY column5", false);
    assertSameAnswerWithAndWithoutLateMaterialization(
        "SET allowReverseOrder = true; " + SELECTION_QUERY + " ORDER BY column5 DESC", false);
  }

  /// Descending without a reverse cursor reads the whole match set, so deferring the other columns is a real saving.
  @Test
  public void testLateMaterializationSortedColumnDesc() {
    assertSameAnswerWithAndWithoutLateMaterialization(
        "SET allowReverseOrder = false; " + SELECTION_QUERY + " ORDER BY column5 DESC", true);
  }

  @Test
  public void testLateMaterializationWithOffsetAndLimit() {
    assertSameAnswerWithAndWithoutLateMaterialization(
        "SET allowReverseOrder = false; " + SELECTION_QUERY + " ORDER BY column5 DESC LIMIT 5 OFFSET 3", true);
  }

  /// An unsorted tail in the order-by removes the limit bound on the scan, so the linear operator benefits too. The
  /// unsorted order-by column is still projected during the scan; only the remaining output columns are deferred.
  @Test
  public void testLateMaterializationWithUnsortedOrderBySuffix() {
    assertSameAnswerWithAndWithoutLateMaterialization(SELECTION_QUERY + " ORDER BY column5, column6", true);
  }

  /// The late fetch builds its own TransformOperator, so a non-order-by expression that is a function must resolve,
  /// both for its values and for its data type in the schema.
  @Test
  public void testLateMaterializationWithTransformProjection() {
    assertSameAnswerWithAndWithoutLateMaterialization(
        "SET allowReverseOrder = false; SELECT column5, ADD(column1, 1), column11 FROM testTable "
            + "ORDER BY column5 DESC", true);
  }
}
