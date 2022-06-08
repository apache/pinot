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
package org.apache.pinot.query.runtime.blocks;

import java.io.IOException;
import java.util.List;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DataBlockTest {
  private static final int TEST_ROW_COUNT = 2;

  @Test
  public void testException()
      throws IOException {
    Exception originalException = new UnsupportedOperationException("Expected test exception.");
    ProcessingException processingException =
        QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, originalException);
    String expected = processingException.getMessage();

    BaseDataBlock dataBlock = DataBlockUtils.getErrorDataBlock(originalException);
    dataBlock.addException(processingException);
    Assert.assertEquals(dataBlock.getDataSchema().getColumnNames().length, 0);
    Assert.assertEquals(dataBlock.getDataSchema().getColumnDataTypes().length, 0);
    Assert.assertEquals(dataBlock.getNumberOfRows(), 0);

    // Assert processing exception and original exception both matches.
    String actual = dataBlock.getExceptions().get(QueryException.QUERY_EXECUTION_ERROR.getErrorCode());
    Assert.assertEquals(actual, expected);
    Assert.assertEquals(dataBlock.getExceptions().get(QueryException.UNKNOWN_ERROR_CODE),
        originalException.getMessage());
  }

  @Test
  public void testAllDataTypes()
      throws IOException {
    DataSchema.ColumnDataType[] columnDataTypes = DataSchema.ColumnDataType.values();
    int numColumns = columnDataTypes.length;
    String[] columnNames = new String[numColumns];
    for (int i = 0; i < numColumns; i++) {
      columnNames[i] = columnDataTypes[i].name();
    }

    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
    List<Object[]> rows = DataBlockTestUtils.getRandomRows(dataSchema, TEST_ROW_COUNT);
    List<Object[]> columnars = DataBlockTestUtils.convertColumnar(dataSchema, rows);
    RowDataBlock rowBlock = DataBlockBuilder.buildFromRows(rows, dataSchema);
    ColumnarDataBlock columnarBlock = DataBlockBuilder.buildFromColumns(columnars, dataSchema);

    for (int colId = 0; colId < dataSchema.getColumnNames().length; colId++) {
      DataSchema.ColumnDataType columnDataType = dataSchema.getColumnDataType(colId);
      for (int rowId = 0; rowId < TEST_ROW_COUNT; rowId++) {
        Object rowVal = DataBlockTestUtils.getElement(rowBlock, rowId, colId, columnDataType);
        Object colVal = DataBlockTestUtils.getElement(columnarBlock, rowId, colId, columnDataType);
        Assert.assertEquals(rowVal, colVal, "Error comparing Row/Column Block at (" + rowId + "," + colId + ")"
            + " of Type: " + columnDataType + "! rowValue: [" + rowVal + "], columnarValue: [" + colVal + "]");
      }
    }
  }
}
