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
package org.apache.pinot.core.common.datablock;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.datablock.ColumnarDataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.RowDataBlock;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.spi.utils.ArrayCopyUtils;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DataBlockTest {
  private static final List<DataSchema.ColumnDataType> EXCLUDE_DATA_TYPES = ImmutableList.of(
      DataSchema.ColumnDataType.OBJECT, DataSchema.ColumnDataType.JSON, DataSchema.ColumnDataType.BYTES,
      DataSchema.ColumnDataType.BYTES_ARRAY);
  private static final int TEST_ROW_COUNT = 5;

  @Test
  public void testException()
      throws IOException {
    Exception originalException = new UnsupportedOperationException("Expected test exception.");
    ProcessingException processingException =
        QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, originalException);
    String expected = processingException.getMessage();

    BaseDataBlock dataBlock = DataBlockUtils.getErrorDataBlock(originalException);
    dataBlock.addException(processingException);
    Assert.assertNull(dataBlock.getDataSchema());
    Assert.assertEquals(dataBlock.getNumberOfRows(), 0);

    // Assert processing exception and original exception both matches.
    String actual = dataBlock.getExceptions().get(QueryException.QUERY_EXECUTION_ERROR.getErrorCode());
    Assert.assertEquals(actual, expected);
    Assert.assertEquals(dataBlock.getExceptions().get(QueryException.UNKNOWN_ERROR_CODE),
        originalException.getMessage());
  }

  /**
   * This test is only here to ensure that {@link org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl}
   * producing data table can actually be wrapped and sent via mailbox in the {@link RowDataBlock} format.
   *
   * @throws Exception
   */
  @Test(dataProvider = "testTypeNullPercentile")
  public void testRowDataBlockCompatibleWithDataTableV4(int nullPercentile)
      throws Exception {
    DataSchema.ColumnDataType[] allDataTypes = DataSchema.ColumnDataType.values();
    List<DataSchema.ColumnDataType> columnDataTypes = new ArrayList<DataSchema.ColumnDataType>();
    List<String> columnNames = new ArrayList<String>();
    for (int i = 0; i < allDataTypes.length; i++) {
      if (!EXCLUDE_DATA_TYPES.contains(allDataTypes[i])) {
        columnNames.add(allDataTypes[i].name());
        columnDataTypes.add(allDataTypes[i]);
      }
    }

    DataSchema dataSchema = new DataSchema(columnNames.toArray(new String[0]),
        columnDataTypes.toArray(new DataSchema.ColumnDataType[0]));
    int numColumns = dataSchema.getColumnDataTypes().length;
    List<Object[]> rows = DataBlockTestUtils.getRandomRows(dataSchema, TEST_ROW_COUNT, nullPercentile);
    DataTableBuilderFactory.setDataTableVersion(DataTableFactory.VERSION_4);
    convertToDataTableCompatibleRows(rows, dataSchema);
    DataTable dataTableImpl = SelectionOperatorUtils.getDataTableFromRows(rows, dataSchema, true);
    BaseDataBlock dataBlockFromDataTable = DataBlockUtils.getDataBlock(ByteBuffer.wrap(dataTableImpl.toBytes()));

    RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
    for (int coldId = 0; coldId < numColumns; coldId++) {
      nullBitmaps[coldId] = dataTableImpl.getNullRowIds(coldId);
    }

    List<Object[]> rowsFromBlock = DataBlockUtils.extractRows(dataBlockFromDataTable);
    for (int rowId = 0; rowId < TEST_ROW_COUNT; rowId++) {
      Object[] rowFromDataTable = SelectionOperatorUtils.extractRowFromDataTableWithNullHandling(dataTableImpl, rowId,
          nullBitmaps);
      Object[] rowFromBlock = rowsFromBlock.get(rowId);
      for (int colId = 0; colId < numColumns; colId++) {
        Object dataTableObj = rowFromDataTable[colId] == null ? null
            : dataSchema.getColumnDataType(colId).convert(rowFromDataTable[colId]);
        Object dataBlockObj = rowFromBlock[colId];
        Assert.assertEquals(dataBlockObj, dataTableObj, "Error comparing Row/Column Block "
            + " at (" + rowId + "," + colId + ") of Type: " + dataSchema.getColumnDataType(colId) + "! "
            + " from DataBlock: [" + dataBlockObj + "], from DataTable: [" + dataTableObj + "]");
      }
    }

    for (int colId = 0; colId < dataSchema.getColumnNames().length; colId++) {
      RoaringBitmap dataBlockBitmap = dataBlockFromDataTable.getNullRowIds(colId);
      RoaringBitmap dataTableBitmap = dataTableImpl.getNullRowIds(colId);
      Assert.assertEquals(dataBlockBitmap, dataTableBitmap);
    }
  }

  private static void convertToDataTableCompatibleRows(List<Object[]> rows, DataSchema dataSchema) {
    int numColumns = dataSchema.getColumnNames().length;
    for (int rowId = 0; rowId < rows.size(); rowId++) {
      for (int colId = 0; colId < numColumns; colId++) {
        switch (dataSchema.getColumnDataType(colId)) {
          case BOOLEAN:
            if (rows.get(rowId)[colId] != null) {
              rows.get(rowId)[colId] = ((boolean) rows.get(rowId)[colId]) ? 1 : 0;
            }
            break;
          case TIMESTAMP:
            if (rows.get(rowId)[colId] != null) {
              rows.get(rowId)[colId] = ((Timestamp) rows.get(rowId)[colId]).getTime();
            }
            break;
          case BOOLEAN_ARRAY:
            if (rows.get(rowId)[colId] != null) {
              boolean[] booleans = (boolean[]) rows.get(rowId)[colId];
              int[] ints = new int[booleans.length];
              ArrayCopyUtils.copy(booleans, ints, booleans.length);
              rows.get(rowId)[colId] = ints;
            }
            break;
          case TIMESTAMP_ARRAY:
            if (rows.get(rowId)[colId] != null) {
              Timestamp[] timestamps = (Timestamp[]) rows.get(rowId)[colId];
              long[] longs = new long[timestamps.length];
              ArrayCopyUtils.copy(timestamps, longs, timestamps.length);
              rows.get(rowId)[colId] = longs;
            }
            break;
          default:
            break;
        }
      }
    }
  }

  @Test(dataProvider = "testTypeNullPercentile")
  public void testAllDataTypes(int nullPercentile)
      throws Exception {

    DataSchema.ColumnDataType[] allDataTypes = DataSchema.ColumnDataType.values();
    List<DataSchema.ColumnDataType> columnDataTypes = new ArrayList<DataSchema.ColumnDataType>();
    List<String> columnNames = new ArrayList<String>();
    for (int i = 0; i < allDataTypes.length; i++) {
      if (!EXCLUDE_DATA_TYPES.contains(allDataTypes[i])) {
        columnNames.add(allDataTypes[i].name());
        columnDataTypes.add(allDataTypes[i]);
      }
    }

    DataSchema dataSchema = new DataSchema(columnNames.toArray(new String[]{}),
        columnDataTypes.toArray(new DataSchema.ColumnDataType[]{}));
    List<Object[]> rows = DataBlockTestUtils.getRandomRows(dataSchema, TEST_ROW_COUNT, nullPercentile);
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

  @DataProvider(name = "testTypeNullPercentile")
  public Object[][] provideTestTypeNullPercentile() {
    return new Object[][]{new Object[]{0}, new Object[]{10}, new Object[]{100}};
  }
}
