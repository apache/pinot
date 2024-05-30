package org.apache.pinot.core.common.datablock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockSerde;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.datablock.OriginalDataBlockSerde;
import org.apache.pinot.common.datablock.ZeroCopyDataBlockSerde;
import org.apache.pinot.common.utils.DataSchema;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DataBlockSerdeTest {

  @Test
  public void testSerdeRowOriginal()
      throws IOException {
    DataBlockUtils.setSerde(DataBlockSerde.Version.V2, new OriginalDataBlockSerde());

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
    byte[] bytes = dataBlock.toBytes();
    DataBlock deserializedDataBlock = DataBlockUtils.deserialize(new ByteBuffer[]{ByteBuffer.wrap(bytes)});
    Assert.assertEquals(deserializedDataBlock, dataBlock);
  }


  @Test
  public void testSerdeColumnOriginal()
      throws IOException {
    DataBlockUtils.setSerde(DataBlockSerde.Version.V2, new OriginalDataBlockSerde());
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
      column[i] = r.nextInt();
    }

    DataBlock dataBlock = DataBlockBuilder.buildFromColumns(Collections.singletonList(column), dataSchema);
    byte[] bytes = dataBlock.toBytes();
    DataBlock deserializedDataBlock = DataBlockUtils.deserialize(new ByteBuffer[]{ByteBuffer.wrap(bytes)});
    Assert.assertEquals(deserializedDataBlock, dataBlock);
  }

  @Test
  public void testSerdeRowZero()
      throws IOException {
    DataBlockUtils.setSerde(DataBlockSerde.Version.V2, new ZeroCopyDataBlockSerde());

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
    byte[] bytes = dataBlock.toBytes();
    DataBlock deserializedDataBlock = DataBlockUtils.deserialize(new ByteBuffer[]{ByteBuffer.wrap(bytes)});
    Assert.assertEquals(deserializedDataBlock, dataBlock);
  }


  @Test
  public void testSerdeColumnZero()
      throws IOException {
    DataBlockUtils.setSerde(DataBlockSerde.Version.V2, new ZeroCopyDataBlockSerde());
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
    byte[] bytes = dataBlock.toBytes();
    DataBlock deserializedDataBlock = DataBlockUtils.deserialize(new ByteBuffer[]{ByteBuffer.wrap(bytes)});
    Assert.assertEquals(deserializedDataBlock, dataBlock);
  }
}
