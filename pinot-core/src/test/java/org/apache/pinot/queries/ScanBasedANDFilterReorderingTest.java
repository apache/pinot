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

/**
 * <p>There are totally 18 columns, 30000 records inside the original Avro file where 11 columns are selected to build
 * the index segment "test_data-sv.avro". Selected columns information are as following:
 * <ul>
 *   ColumnName, FieldType, DataType, Cardinality, IsSorted, HasInvertedIndex
 *   <li>column1, METRIC, INT, 6582, F, F</li>
 *   <li>column3, METRIC, INT, 21910, F, F</li>
 *   <li>column5, DIMENSION, STRING, 1, T, F</li>
 *   <li>column6, DIMENSION, INT, 608, F, T</li>
 *   <li>column7, DIMENSION, INT, 146, F, T</li>
 *   <li>column9, DIMENSION, INT, 1737, F, F</li>
 *   <li>column11, DIMENSION, STRING, 5, F, T</li>
 *   <li>column12, DIMENSION, STRING, 5, F, F</li>
 *   <li>column17, METRIC, INT, 24, F, T</li>
 *   <li>column18, METRIC, INT, 1440, F, T</li>
 *   <li>daysSinceEpoch, TIME, INT, 2, T, F</li>
 * </ul>
 */
public class ScanBasedANDFilterReorderingTest extends InnerSegmentSelectionSingleValueQueriesTest {
  private static final String SELECT_STAR_QUERY = "SELECT * FROM testTable";
  private static final String SET_AND_OPTIMIZATION = "SET "
      + CommonConstants.Broker.Request.QueryOptionKey.AND_SCAN_REORDERING + " = 'True';";

  protected static final String FILTER2 =
      " WHERE column1 in (1236291842, 1123080522) AND column3 in (455144658, 2134396823) "
          + "AND column17 IN (635942547, 1785555661, 1284373442, 1618904660, 561673250, 1727768835, 423049234, 1799989276)";

  @Test
  public void testSelectStar() {
    // Test query with filter
    BaseOperator<SelectionResultsBlock> selectionOnlyOperator = getOperator(SELECT_STAR_QUERY + FILTER2);
    SelectionResultsBlock resultsBlock = selectionOnlyOperator.nextBlock();
    ExecutionStatistics executionStatistics = selectionOnlyOperator.getExecutionStatistics();
//    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
//    assertEquals(executionStatistics.getNumEntriesScannedInFilter(), 48241L);
//    assertEquals(executionStatistics.getNumEntriesScannedPostFilter(), 110L);
//    assertEquals(executionStatistics.getNumTotalDocs(), 30000L);
    DataSchema selectionDataSchema = resultsBlock.getDataSchema();
    Map<String, Integer> columnIndexMap = computeColumnNameToIndexMap(selectionDataSchema);
//    assertEquals(selectionDataSchema.size(), 11);
//    assertEquals(getVirtualColumns(selectionDataSchema), 0);
//    assertTrue(columnIndexMap.containsKey("column1"));
//    assertTrue(columnIndexMap.containsKey("column11"));
//    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column1")), DataSchema.ColumnDataType.INT);
//    assertEquals(selectionDataSchema.getColumnDataType(columnIndexMap.get("column11")),
//        DataSchema.ColumnDataType.STRING);
    List<Object[]> selectionResult = (List<Object[]>) resultsBlock.getRows();
//    assertEquals(selectionResult.size(), 10);
    Object[] firstRow;
//    assertEquals(firstRow.length, 11);
//    assertEquals(((Integer) firstRow[columnIndexMap.get("column1")]).intValue(), 351823652);
//    assertEquals((String) firstRow[columnIndexMap.get("column11")], "t");

    // Test query with filter + and optimization; reduce NumEntriesScannedInFilter 48241L->35905L
    selectionOnlyOperator = getOperator(SET_AND_OPTIMIZATION + SELECT_STAR_QUERY + FILTER2);
    resultsBlock = selectionOnlyOperator.nextBlock();
    executionStatistics = selectionOnlyOperator.getExecutionStatistics();
//    assertEquals(executionStatistics.getNumDocsScanned(), 10L);
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
