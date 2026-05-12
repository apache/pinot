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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datablock.ColumnarDataBlock;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datablock.RowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DataBlockTest {
  private static final List<ColumnDataType> EXCLUDE_DATA_TYPES = List.of(ColumnDataType.JSON, ColumnDataType.OBJECT);
  private static final int TEST_ROW_COUNT = 5;

  @Test
  public void testException() {
    Exception originalException = new UnsupportedOperationException("Expected test exception.");
    String expected = "Expected test error";

    String ex = DataBlockUtils.extractErrorMsg(originalException);
    DataBlock dataBlock = MetadataBlock.newError(-1, -1, null, Map.of(QueryErrorCode.UNKNOWN.getId(), ex));
    dataBlock.addException(QueryErrorCode.QUERY_EXECUTION, expected);
    Assert.assertEquals(dataBlock.getNumberOfRows(), 0);

    // Assert processing exception and original exception both matches.
    String actual = dataBlock.getExceptions().get(QueryErrorCode.QUERY_EXECUTION.getId());
    Assert.assertEquals(actual, expected);
    Assert.assertTrue(
        dataBlock.getExceptions().get(QueryErrorCode.UNKNOWN.getId()).contains(originalException.getMessage()));
  }

  @Test(dataProvider = "testTypeNullPercentile")
  public void testAllDataTypes(int nullPercentile)
      throws IOException {
    ColumnDataType[] allDataTypes = ColumnDataType.values();
    List<ColumnDataType> columnDataTypes = new ArrayList<>();
    List<String> columnNames = new ArrayList<>();
    for (int i = 0; i < allDataTypes.length; i++) {
      if (!EXCLUDE_DATA_TYPES.contains(allDataTypes[i])) {
        columnNames.add(allDataTypes[i].name());
        columnDataTypes.add(allDataTypes[i]);
      }
    }

    DataSchema dataSchema =
        new DataSchema(columnNames.toArray(new String[]{}), columnDataTypes.toArray(new ColumnDataType[]{}));
    List<Object[]> rows = DataBlockTestUtils.getRandomRows(dataSchema, TEST_ROW_COUNT, nullPercentile);
    List<Object[]> columnars = DataBlockTestUtils.convertColumnar(dataSchema, rows);
    RowDataBlock rowBlock = DataBlockBuilder.buildFromRows(rows, dataSchema);
    ColumnarDataBlock columnarBlock = DataBlockBuilder.buildFromColumns(columnars, dataSchema);

    for (int colId = 0; colId < dataSchema.getColumnNames().length; colId++) {
      ColumnDataType columnDataType = dataSchema.getColumnDataType(colId);
      for (int rowId = 0; rowId < TEST_ROW_COUNT; rowId++) {
        Object rowVal;
        Object colVal;
        try {
          try {
            rowVal = DataBlockTestUtils.getElement(rowBlock, rowId, colId, columnDataType);
          } catch (AssertionError e) {
            throw new AssertionError(
                "Error comparing Row/Column Block at (" + rowId + "," + colId + ") of Type: " + columnDataType
                    + ". Cannot get element on row block!", e);
          }
          try {
            colVal = DataBlockTestUtils.getElement(columnarBlock, rowId, colId, columnDataType);
          } catch (AssertionError e) {
            throw new AssertionError(
                "Error comparing Row/Column Block at (" + rowId + "," + colId + ") of Type: " + columnDataType
                    + ". Cannot get element on columnar block!", e);
          }
          Assert.assertEquals(rowVal, colVal,
              "Error comparing Row/Column Block at (" + rowId + "," + colId + ")" + " of Type: " + columnDataType
                  + "! rowValue: [" + rowVal + "], columnarValue: [" + colVal + "]");
        } catch (RuntimeException e) {
          throw new RuntimeException(
              "Error comparing Row/Column Block at (" + rowId + "," + colId + ") of Type: " + columnDataType + "!", e);
        }
      }
    }
  }

  @DataProvider(name = "testTypeNullPercentile")
  public Object[][] provideTestTypeNullPercentile() {
    return new Object[][]{new Object[]{0}, new Object[]{10}, new Object[]{100}};
  }

  @Test
  public void intArraySerDe()
      throws IOException {
    Object[] row = new Object[]{new int[0]};
    List<Object[]> rows = new ArrayList<>();
    rows.add(row);
    DataSchema dataSchema = new DataSchema(new String[]{"intArray"}, new ColumnDataType[]{ColumnDataType.INT_ARRAY});
    DataBlock dataBlock = DataBlockBuilder.buildFromRows(rows, dataSchema);
    List<ByteBuffer> serialized = dataBlock.serialize();
    DataBlock deserialized = DataBlockUtils.deserialize(serialized);
    int[] intArray = deserialized.getIntArray(0, 0);
    Assert.assertEquals(intArray.length, 0);
  }

  @Test
  public void bytesArraySerDe()
      throws IOException {
    ByteArray[] expected = new ByteArray[]{new ByteArray(new byte[]{0xD, 0xA}), new ByteArray(new byte[]{0xB, 0xE})};
    Object[] row = new Object[]{expected};
    List<Object[]> rows = new ArrayList<>();
    rows.add(row);
    DataSchema dataSchema = new DataSchema(new String[]{"byteArray"}, new ColumnDataType[]{ColumnDataType.BYTES_ARRAY});
    DataBlock dataBlock = DataBlockBuilder.buildFromRows(rows, dataSchema);
    List<ByteBuffer> serialized = dataBlock.serialize();
    DataBlock deserialized = DataBlockUtils.deserialize(serialized);
    Assert.assertEquals(deserialized.getBytesArray(0, 0), expected);
  }
}
