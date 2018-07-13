/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.queries;

import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.core.operator.ExecutionStatistics;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.query.EmptySelectionOperator;
import com.linkedin.pinot.core.operator.query.SelectionOnlyOperator;
import com.linkedin.pinot.core.operator.query.SelectionOrderByOperator;
import java.io.Serializable;
import java.util.List;
import java.util.Queue;
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
    EmptySelectionOperator emptySelectionOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = emptySelectionOperator.nextBlock();
    ExecutionStatistics executionStatistics = emptySelectionOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    DataSchema selectionDataSchema = resultsBlock.getSelectionDataSchema();
    Assert.assertEquals(selectionDataSchema.size(), 10);
    Assert.assertEquals(selectionDataSchema.getColumnName(0), "column1");
    Assert.assertEquals(selectionDataSchema.getColumnName(5), "column6");
    Assert.assertEquals(selectionDataSchema.getColumnDataType(0), DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(5), DataSchema.ColumnDataType.INT_ARRAY);
    Assert.assertTrue(resultsBlock.getSelectionResult().isEmpty());

    // Test query with filter
    emptySelectionOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = emptySelectionOperator.nextBlock();
    executionStatistics = emptySelectionOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    selectionDataSchema = resultsBlock.getSelectionDataSchema();
    Assert.assertEquals(selectionDataSchema.size(), 10);
    Assert.assertEquals(selectionDataSchema.getColumnName(0), "column1");
    Assert.assertEquals(selectionDataSchema.getColumnName(5), "column6");
    Assert.assertEquals(selectionDataSchema.getColumnDataType(0), DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(5), DataSchema.ColumnDataType.INT_ARRAY);
    Assert.assertTrue(resultsBlock.getSelectionResult().isEmpty());
  }

  @Test
  public void testSelectStar() {
    String query = "SELECT * FROM testTable";

    // Test query without filter
    SelectionOnlyOperator selectionOnlyOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = selectionOnlyOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 100L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    DataSchema selectionDataSchema = resultsBlock.getSelectionDataSchema();
    Assert.assertEquals(selectionDataSchema.size(), 10);
    Assert.assertEquals(selectionDataSchema.getColumnName(0), "column1");
    Assert.assertEquals(selectionDataSchema.getColumnName(5), "column6");
    Assert.assertEquals(selectionDataSchema.getColumnDataType(0), DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(5), DataSchema.ColumnDataType.INT_ARRAY);
    List<Serializable[]> selectionResult = (List<Serializable[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    Serializable[] firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 10);
    Assert.assertEquals(((Integer) firstRow[0]).intValue(), 890282370);
    Assert.assertEquals(firstRow[5], new int[]{2147483647});

    // Test query with filter
    selectionOnlyOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = selectionOnlyOperator.nextBlock();
    executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 230501L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 100L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    selectionDataSchema = resultsBlock.getSelectionDataSchema();
    Assert.assertEquals(selectionDataSchema.size(), 10);
    Assert.assertEquals(selectionDataSchema.getColumnName(0), "column1");
    Assert.assertEquals(selectionDataSchema.getColumnName(5), "column6");
    Assert.assertEquals(selectionDataSchema.getColumnDataType(0), DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(5), DataSchema.ColumnDataType.INT_ARRAY);
    selectionResult = (List<Serializable[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 10);
    Assert.assertEquals(((Integer) firstRow[0]).intValue(), 890282370);
    Assert.assertEquals(firstRow[5], new int[]{2147483647});
  }

  @Test
  public void testSelectionOnly() {
    String query = "SELECT" + SELECTION + " FROM testTable";

    // Test query without filter
    SelectionOnlyOperator selectionOnlyOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = selectionOnlyOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 30L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    DataSchema selectionDataSchema = resultsBlock.getSelectionDataSchema();
    Assert.assertEquals(selectionDataSchema.size(), 3);
    Assert.assertEquals(selectionDataSchema.getColumnName(0), "column1");
    Assert.assertEquals(selectionDataSchema.getColumnName(2), "column6");
    Assert.assertEquals(selectionDataSchema.getColumnDataType(0), DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(2), DataSchema.ColumnDataType.INT_ARRAY);
    List<Serializable[]> selectionResult = (List<Serializable[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    Serializable[] firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 3);
    Assert.assertEquals(((Integer) firstRow[0]).intValue(), 890282370);
    Assert.assertEquals(firstRow[2], new int[]{2147483647});

    // Test query with filter
    selectionOnlyOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = selectionOnlyOperator.nextBlock();
    executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 230501L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 30L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    selectionDataSchema = resultsBlock.getSelectionDataSchema();
    Assert.assertEquals(selectionDataSchema.size(), 3);
    Assert.assertEquals(selectionDataSchema.getColumnName(0), "column1");
    Assert.assertEquals(selectionDataSchema.getColumnName(2), "column6");
    Assert.assertEquals(selectionDataSchema.getColumnDataType(0), DataSchema.ColumnDataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(2), DataSchema.ColumnDataType.INT_ARRAY);
    selectionResult = (List<Serializable[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 3);
    Assert.assertEquals(((Integer) firstRow[0]).intValue(), 890282370);
    Assert.assertEquals(firstRow[2], new int[]{2147483647});
  }

  @Test
  public void testSelectionOrderBy() {
    String query = "SELECT" + SELECTION + " FROM testTable" + ORDER_BY;

    // Test query without filter
    SelectionOrderByOperator selectionOrderByOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 100000L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 400000L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    DataSchema selectionDataSchema = resultsBlock.getSelectionDataSchema();
    Assert.assertEquals(selectionDataSchema.size(), 4);
    Assert.assertEquals(selectionDataSchema.getColumnName(0), "column5");
    Assert.assertEquals(selectionDataSchema.getColumnName(3), "column6");
    Assert.assertEquals(selectionDataSchema.getColumnDataType(0), DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(3), DataSchema.ColumnDataType.INT_ARRAY);
    Queue<Serializable[]> selectionResult = (Queue<Serializable[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    Serializable[] lastRow = selectionResult.peek();
    Assert.assertEquals(lastRow.length, 4);
    Assert.assertEquals((String) lastRow[0], "AKXcXcIqsqOJFsdwxZ");
    Assert.assertEquals(lastRow[3], new int[]{1252});

    // Test query with filter
    selectionOrderByOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 15620L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 282430L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 62480L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 100000L);
    selectionDataSchema = resultsBlock.getSelectionDataSchema();
    Assert.assertEquals(selectionDataSchema.size(), 4);
    Assert.assertEquals(selectionDataSchema.getColumnName(0), "column5");
    Assert.assertEquals(selectionDataSchema.getColumnName(3), "column6");
    Assert.assertEquals(selectionDataSchema.getColumnDataType(0), DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(selectionDataSchema.getColumnDataType(3), DataSchema.ColumnDataType.INT_ARRAY);
    selectionResult = (Queue<Serializable[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    lastRow = selectionResult.peek();
    Assert.assertEquals(lastRow.length, 4);
    Assert.assertEquals((String) lastRow[0], "AKXcXcIqsqOJFsdwxZ");
    Assert.assertEquals(lastRow[3], new int[]{2147483647});
  }
}
