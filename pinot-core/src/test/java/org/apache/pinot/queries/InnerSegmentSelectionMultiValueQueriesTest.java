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
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.EmptySelectionOperator;
import org.apache.pinot.spi.data.DataSchema;
import org.testng.Assert;
import org.testng.annotations.Test;


@SuppressWarnings("ConstantConditions")
public class InnerSegmentSelectionMultiValueQueriesTest extends BaseMultiValueQueriesTest {
  private static final String SELECTION = " column1, column5, column6";
  private static final String ORDER_BY = " ORDER BY column5, column9";

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
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    Assert.assertEquals(selectionDataSchema.size(), 10);
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertTrue(columnIndexMap.containsKey("column6"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
    Assert.assertTrue(resultsBlock.getSelectionResult().isEmpty());

    // Test query with filter
    emptySelectionOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = emptySelectionOperator.nextBlock();
    executionStatistics = emptySelectionOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);
    Assert.assertEquals(selectionDataSchema.size(), 10);
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertTrue(columnIndexMap.containsKey("column6"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
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
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 100L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    Assert.assertEquals(selectionDataSchema.size(), 10);
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertTrue(columnIndexMap.containsKey("column6"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
    List<Object[]> selectionResult = (List<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    Object[] firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 10);
    Assert.assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 890282370);
    Assert.assertEquals(firstRow[columnIndexMap.get("column6")], new int[]{2147483647});

    // Test query with filter
    selectionOnlyOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = selectionOnlyOperator.nextBlock();
    executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 79L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 100L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    Assert.assertEquals(selectionDataSchema.size(), 10);
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertTrue(columnIndexMap.containsKey("column6"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
    selectionResult = (List<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 10);
    Assert.assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 890282370);
    Assert.assertEquals(firstRow[columnIndexMap.get("column6")], new int[]{2147483647});
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
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    Assert.assertEquals(selectionDataSchema.size(), 3);
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertTrue(columnIndexMap.containsKey("column6"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
    List<Object[]> selectionResult = (List<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    Object[] firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 3);
    Assert.assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 890282370);
    Assert.assertEquals(firstRow[columnIndexMap.get("column6")], new int[]{2147483647});

    // Test query with filter
    selectionOnlyOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = selectionOnlyOperator.nextBlock();
    executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 79L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 30L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    Assert.assertEquals(selectionDataSchema.size(), 3);
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertTrue(columnIndexMap.containsKey("column6"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
    selectionResult = (List<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 3);
    Assert.assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 890282370);
    Assert.assertEquals(firstRow[columnIndexMap.get("column6")], new int[]{2147483647});
  }

  @Test
  public void testSelectionOrderBy() {
    String query = "SELECT" + SELECTION + " FROM testTable" + ORDER_BY;

    // Test query without filter
    BaseOperator<IntermediateResultsBlock> selectionOrderByOperator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 100000L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    // 100000 * (2 order-by columns + 1 docId column) + 10 * (2 non-order-by columns)
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 300020L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    Assert.assertEquals(selectionDataSchema.size(), 4);
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertTrue(columnIndexMap.containsKey("column6"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
    PriorityQueue<Object[]> selectionResult = (PriorityQueue<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    Object[] lastRow = selectionResult.peek();
    Assert.assertEquals(lastRow.length, 4);
    Assert.assertEquals((String) lastRow[columnIndexMap.get("column5")], "AKXcXcIqsqOJFsdwxZ");
    Assert.assertEquals(lastRow[columnIndexMap.get("column6")], new int[]{1252});

    // Test query with filter
    selectionOrderByOperator = getOperatorForPqlQueryWithFilter(query);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 15620L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 275416L);
    // 15620 * (2 order-by columns + 1 docId column) + 10 * (2 non-order-by columns)
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 46880L);
    Assert.assertEquals(executionStatistics.getNumTotalDocs(), 100000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);

    Assert.assertEquals(selectionDataSchema.size(), 4);
    Assert.assertTrue(columnIndexMap.containsKey("column1"));
    Assert.assertTrue(columnIndexMap.containsKey("column6"));
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")),
        DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column6")),
        DataSchema.ColumnDataType.INT_ARRAY);
    selectionResult = (PriorityQueue<Object[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    lastRow = selectionResult.peek();
    Assert.assertEquals(lastRow.length, 4);
    Assert.assertEquals((String) lastRow[columnIndexMap.get("column5")], "AKXcXcIqsqOJFsdwxZ");
    Assert.assertEquals(lastRow[columnIndexMap.get("column6")], new int[]{2147483647});
  }

  private Map<String, Integer> computeColumnNameToIndexMap(DataSchema dataSchema) {
    Map<String, Integer> columnIndexMap = new HashMap<>();

    for (int i = 0; i < dataSchema.size(); i++) {
      columnIndexMap.put(dataSchema.getColumnName(i), i);
    }
    return columnIndexMap;
  }
}
