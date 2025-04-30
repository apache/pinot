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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockEquals;
import org.apache.pinot.common.datablock.DataBlockSerde;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.ZeroCopyDataBlockSerde;
import org.apache.pinot.common.utils.DataSchema;
import org.testng.annotations.Test;

public class DataBlockSerdeTest {

  @Test
  public void testSerdeRowZero()
      throws IOException {
    DataBlockUtils.setSerde(DataBlockSerde.Version.V1_V2, new ZeroCopyDataBlockSerde());

    int numRows = 1000;
    DataSchema dataSchema = new DataSchema(
        new String[]{"value"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});

    List<Object[]> rows = new ArrayList<>(numRows);
    Random r = new Random(42);
    for (int i = 0; i < numRows; i++) {
      rows.add(new Object[]{r.nextInt()});
    }

    DataBlock dataBlock = DataBlockBuilder.buildFromRows(rows, dataSchema);
    List<ByteBuffer> serialize = DataBlockUtils.serialize(DataBlockSerde.Version.V1_V2, dataBlock);
    DataBlock deserializedDataBlock = DataBlockUtils.deserialize(serialize);
    DataBlockEquals.checkSameContent(deserializedDataBlock, dataBlock,
        "Unexpected value after serialization and deserialization");
  }


  @Test
  public void testSerdeColumnZero()
      throws IOException {
    DataBlockUtils.setSerde(DataBlockSerde.Version.V1_V2, new ZeroCopyDataBlockSerde());
    int numRows = 1000;
    DataSchema dataSchema = new DataSchema(
        new String[]{"value"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});

    Object[] column = new Object[numRows];
    Random r = new Random(42);
    for (int i = 0; i < numRows; i++) {
      if (r.nextInt(100) < 10) {
        column[i] = null;
        continue;
      }
      column[i] = r.nextInt(100);
    }

    DataBlock dataBlock = DataBlockBuilder.buildFromColumns(Collections.singletonList(column), dataSchema);
    List<ByteBuffer> serialize = DataBlockUtils.serialize(DataBlockSerde.Version.V1_V2, dataBlock);
    DataBlock deserializedDataBlock = DataBlockUtils.deserialize(serialize);
    DataBlockEquals.checkSameContent(deserializedDataBlock, dataBlock,
        "Unexpected value after serialization and deserialization");
  }
}
