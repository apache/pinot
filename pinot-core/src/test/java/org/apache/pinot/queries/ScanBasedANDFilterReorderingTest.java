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

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ScanBasedANDFilterReorderingTest extends InnerSegmentSelectionSingleValueQueriesTest {
  private static final String SELECT_STAR_QUERY = "SELECT * FROM testTable";
  private static final String SET_AND_OPTIMIZATION = "SET "
      + CommonConstants.Broker.Request.QueryOptionKey.AND_SCAN_REORDERING + " = 'True';";

  @Test
  public void testSelectStar() {
    // Test query with filter
    BaseOperator<SelectionResultsBlock> selectionOnlyOperator = getOperator(SELECT_STAR_QUERY + FILTER);
    SelectionResultsBlock resultsBlock = selectionOnlyOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 48241L);
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 110L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);
    assertEquals(selectionDataSchema.size(), 11);
    assertEquals(getVirtualColumns(selectionDataSchema), 0);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column11"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), DataSchema.ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column11")),
        DataSchema.ColumnDataType.STRING);
    List<Object[]> selectionResult = (List<Object[]>) resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    Object[] firstRow = selectionResult.get(0);
    assertEquals(firstRow.length, 11);
    assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 351823652);
    assertEquals((String) firstRow[columnIndexMap.get("column11")], "t");

    // Test query with filter + and optimization; reduce NumEntriesScannedInFilter 48241L->35905L
    selectionOnlyOperator = getOperator(SET_AND_OPTIMIZATION + SELECT_STAR_QUERY + FILTER);
    resultsBlock = selectionOnlyOperator.nextBlock();
    executionStatistics = selectionOnlyOperator.getExecutionStatistics();
    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 48241L);
    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 110L);
    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    selectionDataSchema = resultsBlock.getDataSchema();
    columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);
    assertEquals(selectionDataSchema.size(), 11);
    assertEquals(getVirtualColumns(selectionDataSchema), 0);
    assertTrue(columnIndexMap.containsKey("column1"));
    assertTrue(columnIndexMap.containsKey("column11"));
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), DataSchema.ColumnDataType.INT);
    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column11")),
        DataSchema.ColumnDataType.STRING);
    selectionResult = (List<Object[]>) resultsBlock.getRows();
    assertEquals(selectionResult.size(), 10);
    firstRow = selectionResult.get(0);
    assertEquals(firstRow.length, 11);
    assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 351823652);
    assertEquals((String) firstRow[columnIndexMap.get("column11")], "t");
  }
}
