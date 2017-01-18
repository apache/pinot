/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.core.operator.ExecutionStatistics;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.query.MSelectionOnlyOperator;
import com.linkedin.pinot.core.operator.query.MSelectionOrderByOperator;
import java.io.Serializable;
import java.util.List;
import java.util.Queue;
import org.testng.Assert;
import org.testng.annotations.Test;


@SuppressWarnings("ConstantConditions")
public class InnerSegmentSelectionSingleValueQueriesTest extends BaseSingleValueQueriesTest {
  private static final String SELECTION = " column1, column5, column11";
  private static final String ORDER_BY = " ORDER BY column6, column1";

  @Test
  public void testSelectStar() {
    String query = "SELECT * FROM testTable";

    // Test query without filter.
    MSelectionOnlyOperator selectionOnlyOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = (IntermediateResultsBlock) selectionOnlyOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 110L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getSelectionDataSchema();
    Assert.assertEquals(selectionDataSchema.size(), 11);
    Assert.assertEquals(selectionDataSchema.getColumnName(0), "column1");
    Assert.assertEquals(selectionDataSchema.getColumnName(1), "column11");
    Assert.assertEquals(selectionDataSchema.getColumnType(0), FieldSpec.DataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnType(1), FieldSpec.DataType.STRING);
    List<Serializable[]> selectionResult = (List<Serializable[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    Serializable[] firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 11);
    Assert.assertEquals(((Integer) firstRow[0]).intValue(), 1578964907);
    Assert.assertEquals((String) firstRow[1], "P");

    // Test query with filter.
    selectionOnlyOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = (IntermediateResultsBlock) selectionOnlyOperator.nextBlock();
    executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 48241L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 110L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 30000L);
    selectionDataSchema = resultsBlock.getSelectionDataSchema();
    Assert.assertEquals(selectionDataSchema.size(), 11);
    Assert.assertEquals(selectionDataSchema.getColumnName(0), "column1");
    Assert.assertEquals(selectionDataSchema.getColumnName(1), "column11");
    Assert.assertEquals(selectionDataSchema.getColumnType(0), FieldSpec.DataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnType(1), FieldSpec.DataType.STRING);
    selectionResult = (List<Serializable[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 11);
    Assert.assertEquals(((Integer) firstRow[0]).intValue(), 351823652);
    Assert.assertEquals((String) firstRow[1], "t");
  }

  @Test
  public void testSelectionOnly() {
    String query = "SELECT" + SELECTION + " FROM testTable";

    // Test query without filter.
    MSelectionOnlyOperator selectionOnlyOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = (IntermediateResultsBlock) selectionOnlyOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 30L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getSelectionDataSchema();
    Assert.assertEquals(selectionDataSchema.size(), 3);
    Assert.assertEquals(selectionDataSchema.getColumnName(0), "column1");
    Assert.assertEquals(selectionDataSchema.getColumnName(2), "column11");
    Assert.assertEquals(selectionDataSchema.getColumnType(0), FieldSpec.DataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnType(1), FieldSpec.DataType.STRING);
    List<Serializable[]> selectionResult = (List<Serializable[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    Serializable[] firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 3);
    Assert.assertEquals(((Integer) firstRow[0]).intValue(), 1578964907);
    Assert.assertEquals((String) firstRow[2], "P");

    // Test query with filter.
    selectionOnlyOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = (IntermediateResultsBlock) selectionOnlyOperator.nextBlock();
    executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 48241L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 30L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 30000L);
    selectionDataSchema = resultsBlock.getSelectionDataSchema();
    Assert.assertEquals(selectionDataSchema.size(), 3);
    Assert.assertEquals(selectionDataSchema.getColumnName(0), "column1");
    Assert.assertEquals(selectionDataSchema.getColumnName(2), "column11");
    Assert.assertEquals(selectionDataSchema.getColumnType(0), FieldSpec.DataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnType(1), FieldSpec.DataType.STRING);
    selectionResult = (List<Serializable[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    firstRow = selectionResult.get(0);
    Assert.assertEquals(firstRow.length, 3);
    Assert.assertEquals(((Integer) firstRow[0]).intValue(), 351823652);
    Assert.assertEquals((String) firstRow[2], "t");
  }

  @Test
  public void testSelectionOrderBy() {
    String query = "SELECT" + SELECTION + " FROM testTable" + ORDER_BY;

    // Test query without filter.
    MSelectionOrderByOperator selectionOrderByOperator = getOperatorForQuery(query);
    IntermediateResultsBlock resultsBlock = (IntermediateResultsBlock) selectionOrderByOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 30000L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 0L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 120000L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getSelectionDataSchema();
    Assert.assertEquals(selectionDataSchema.size(), 4);
    Assert.assertEquals(selectionDataSchema.getColumnName(0), "column6");
    Assert.assertEquals(selectionDataSchema.getColumnName(1), "column1");
    Assert.assertEquals(selectionDataSchema.getColumnType(0), FieldSpec.DataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnType(1), FieldSpec.DataType.INT);
    Queue<Serializable[]> selectionResult = (Queue<Serializable[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    Serializable[] lastRow = selectionResult.peek();
    Assert.assertEquals(lastRow.length, 4);
    Assert.assertEquals(((Integer) lastRow[0]).intValue(), 6043515);
    Assert.assertEquals(((Integer) lastRow[1]).intValue(), 10542595);

    // Test query with filter.
    selectionOrderByOperator = getOperatorForQueryWithFilter(query);
    resultsBlock = (IntermediateResultsBlock) selectionOrderByOperator.nextBlock();
    executionStatistics = selectionOrderByOperator.getExecutionStatistics();
    Assert.assertEquals(executionStatistics.getNumDocsScanned(), 6129L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 84134L);
    Assert.assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 24516L);
    Assert.assertEquals(executionStatistics.getNumTotalRawDocs(), 30000L);
    selectionDataSchema = resultsBlock.getSelectionDataSchema();
    Assert.assertEquals(selectionDataSchema.size(), 4);
    Assert.assertEquals(selectionDataSchema.getColumnName(0), "column6");
    Assert.assertEquals(selectionDataSchema.getColumnName(1), "column1");
    Assert.assertEquals(selectionDataSchema.getColumnType(0), FieldSpec.DataType.INT);
    Assert.assertEquals(selectionDataSchema.getColumnType(1), FieldSpec.DataType.INT);
    selectionResult = (Queue<Serializable[]>) resultsBlock.getSelectionResult();
    Assert.assertEquals(selectionResult.size(), 10);
    lastRow = selectionResult.peek();
    Assert.assertEquals(lastRow.length, 4);
    Assert.assertEquals(((Integer) lastRow[0]).intValue(), 6043515);
    Assert.assertEquals(((Integer) lastRow[1]).intValue(), 462769197);
  }
}
