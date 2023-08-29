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
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datablock.RowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datablock.DataBlockBuilder;
import org.apache.pinot.core.common.datablock.DataBlockTestUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TransferableBlockUtilsTest {
  private static final int TOTAL_ROW_COUNT = 50;
  private static final int TEST_EST_BYTES_PER_COLUMN = 8;
  private static final List<DataSchema.ColumnDataType> EXCLUDE_DATA_TYPES = ImmutableList.of(
      DataSchema.ColumnDataType.OBJECT, DataSchema.ColumnDataType.JSON, DataSchema.ColumnDataType.BYTES,
      DataSchema.ColumnDataType.BYTES_ARRAY);

  private static DataSchema getDataSchema() {
    DataSchema.ColumnDataType[] allDataTypes = DataSchema.ColumnDataType.values();
    List<DataSchema.ColumnDataType> columnDataTypes = new ArrayList<DataSchema.ColumnDataType>();
    List<String> columnNames = new ArrayList<String>();
    for (int i = 0; i < allDataTypes.length; i++) {
      if (!EXCLUDE_DATA_TYPES.contains(allDataTypes[i])) {
        columnNames.add(allDataTypes[i].name());
        columnDataTypes.add(allDataTypes[i]);
      }
    }
    return new DataSchema(columnNames.toArray(new String[0]),
        columnDataTypes.toArray(new DataSchema.ColumnDataType[0]));
  }

  @DataProvider
  public static Object[][] splitRowCountProvider() {
    // test against smaller than or larger than total data.
    return new Object[][]{new Object[]{1}, new Object[]{10}, new Object[]{42}, new Object[]{100}};
  }

  // Test that we only send one block when block size is smaller than maxBlockSize.
  @Test(dataProvider = "splitRowCountProvider")
  public void testSplitBlockUtils(int splitRowCount)
      throws Exception {
    DataSchema dataSchema = getDataSchema();
    // compare serialized split
    int estRowSizeInBytes = dataSchema.size() * TEST_EST_BYTES_PER_COLUMN;
    List<Object[]> rows = DataBlockTestUtils.getRandomRows(dataSchema, TOTAL_ROW_COUNT, 1);
    RowDataBlock rowBlock = DataBlockBuilder.buildFromRows(rows, dataSchema);
    validateBlocks(TransferableBlockUtils.splitBlock(new TransferableBlock(rowBlock),
        DataBlock.Type.ROW, estRowSizeInBytes * splitRowCount + 1), rows, dataSchema);
    // compare non-serialized split
    validateBlocks(TransferableBlockUtils.splitBlock(new TransferableBlock(rows, dataSchema, DataBlock.Type.ROW),
        DataBlock.Type.ROW, estRowSizeInBytes * splitRowCount + 1), rows, dataSchema);
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

    // METADATA
    MetadataBlock metadataBlock = new MetadataBlock(MetadataBlock.MetadataBlockType.EOS);
    validateNonSplittableBlock(metadataBlock);
  }

  private void validateNonSplittableBlock(BaseDataBlock nonSplittableBlock)
      throws Exception {
    Iterator<TransferableBlock> transferableBlocks =
        TransferableBlockUtils.splitBlock(new TransferableBlock(nonSplittableBlock), DataBlock.Type.METADATA,
            4 * 1024 * 1024);
    Assert.assertTrue(transferableBlocks.hasNext());
    Assert.assertSame(transferableBlocks.next().getDataBlock(), nonSplittableBlock);
    Assert.assertFalse(transferableBlocks.hasNext());
  }

  private void validateBlocks(Iterator<TransferableBlock> blocks, List<Object[]> rows, DataSchema dataSchema)
      throws Exception {
    int rowId = 0;
    while (blocks.hasNext()) {
      TransferableBlock block = blocks.next();
      for (Object[] row : block.getContainer()) {
        for (int colId = 0; colId < dataSchema.getColumnNames().length; colId++) {
          if (row[colId] == null && rows.get(rowId)[colId] == null) {
            continue;
          }
          DataSchema.ColumnDataType columnDataType = dataSchema.getColumnDataType(colId);
          Object actualVal = row[colId];
          Object expectedVal = rows.get(rowId)[colId];
          Assert.assertEquals(actualVal, expectedVal, "Error comparing split Block at (" + rowId + "," + colId + ")"
              + " of Type: " + columnDataType + "! expected: [" + expectedVal + "], actual: [" + actualVal + "]");
        }
        rowId++;
      }
    }
  }
}
