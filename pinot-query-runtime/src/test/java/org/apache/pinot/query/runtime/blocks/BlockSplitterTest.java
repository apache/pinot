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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.datablock.ColumnarDataBlock;
import org.apache.pinot.common.datablock.RowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.core.common.datablock.DataBlockTestUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType;


public class BlockSplitterTest {
  private static final int TOTAL_ROW_COUNT = 50;
  private static final int TEST_EST_BYTES_PER_COLUMN = 8;
  private static final List<ColumnDataType> EXCLUDE_DATA_TYPES =
      ImmutableList.of(ColumnDataType.OBJECT, ColumnDataType.JSON, ColumnDataType.BYTES, ColumnDataType.BYTES_ARRAY);

  private static DataSchema getDataSchema() {
    ColumnDataType[] allDataTypes = ColumnDataType.values();
    List<ColumnDataType> columnDataTypes = new ArrayList<>();
    List<String> columnNames = new ArrayList<>();
    for (int i = 0; i < allDataTypes.length; i++) {
      if (!EXCLUDE_DATA_TYPES.contains(allDataTypes[i])) {
        columnNames.add(allDataTypes[i].name());
        columnDataTypes.add(allDataTypes[i]);
      }
    }
    return new DataSchema(columnNames.toArray(new String[0]), columnDataTypes.toArray(new ColumnDataType[0]));
  }

  @DataProvider
  public static Object[][] splitRowCountProvider() {
    // test against smaller than or larger than total data.
    return new Object[][]{new Object[]{1}, new Object[]{10}, new Object[]{42}, new Object[]{100}};
  }

  private void testSplitRows(List<Object[]> rows, MseBlock.Data rowBlock, int splitRowCount) {
    DataSchema dataSchema = getDataSchema();
    // compare serialized split
    int estRowSizeInBytes = dataSchema.size() * TEST_EST_BYTES_PER_COLUMN;
    int maxBlockSize = estRowSizeInBytes * splitRowCount + 1;
    Iterator<? extends MseBlock.Data> split = BlockSplitter.DEFAULT.split(rowBlock, maxBlockSize);
    validateBlocks(split, rows, dataSchema);
  }

  // Test that we only send one block when block size is smaller than maxBlockSize.
  @Test(dataProvider = "splitRowCountProvider")
  public void testSplitHeapRows(int splitRowCount)
      throws Exception {
    DataSchema dataSchema = getDataSchema();
    List<Object[]> rows = DataBlockTestUtils.getRandomRows(dataSchema, TOTAL_ROW_COUNT, 1);
    RowHeapDataBlock heapBlock = new RowHeapDataBlock(rows, dataSchema);
    testSplitRows(rows, heapBlock, splitRowCount);
  }

  // Test that we only send one block when block size is smaller than maxBlockSize.
  @Test(dataProvider = "splitRowCountProvider")
  public void testSplitSerializesRows(int splitRowCount)
      throws Exception {
    DataSchema dataSchema = getDataSchema();
    List<Object[]> rows = DataBlockTestUtils.getRandomRows(dataSchema, TOTAL_ROW_COUNT, 1);
    RowDataBlock dataBlock = DataBlockBuilder.buildFromRows(rows, dataSchema);
    SerializedDataBlock serializedBlock = new SerializedDataBlock(dataBlock);
    testSplitRows(rows, serializedBlock, splitRowCount);
  }

  @Test
  public void testNonSplittableBlock()
      throws Exception {
    // COLUMNAR
    DataSchema dataSchema = getDataSchema();
    List<Object[]> columnars = DataBlockTestUtils.convertColumnar(dataSchema,
        DataBlockTestUtils.getRandomRows(dataSchema, TOTAL_ROW_COUNT, 1));
    ColumnarDataBlock columnarBlock = DataBlockBuilder.buildFromColumns(columnars, dataSchema);
    validateNonSplittableBlock(columnarBlock);
  }

  private void validateNonSplittableBlock(BaseDataBlock nonSplittableBlock) {
    SerializedDataBlock block = new SerializedDataBlock(nonSplittableBlock);
    Iterator<? extends MseBlock.Data> mseBlocks = BlockSplitter.DEFAULT.split(block, 4 * 1024 * 1024);
    Assert.assertTrue(mseBlocks.hasNext());
    Assert.assertSame(mseBlocks.next(), block);
    Assert.assertFalse(mseBlocks.hasNext());
  }

  private void validateBlocks(Iterator<? extends MseBlock> blocks, List<Object[]> rows, DataSchema dataSchema) {
    int rowId = 0;
    while (blocks.hasNext()) {
      MseBlock block = blocks.next();
      Assert.assertTrue(block.isData());
      for (Object[] row : ((MseBlock.Data) block).asRowHeap().getRows()) {
        for (int colId = 0; colId < dataSchema.getColumnNames().length; colId++) {
          if (row[colId] == null && rows.get(rowId)[colId] == null) {
            continue;
          }
          ColumnDataType columnDataType = dataSchema.getColumnDataType(colId);
          Object actualVal = row[colId];
          Object expectedVal = rows.get(rowId)[colId];
          Assert.assertEquals(actualVal, expectedVal,
              "Error comparing split Block at (" + rowId + "," + colId + ")" + " of Type: " + columnDataType
                  + "! expected: [" + expectedVal + "], actual: [" + actualVal + "]");
        }
        rowId++;
      }
    }
  }
}
