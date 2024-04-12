package org.apache.pinot.common.datablock;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pinot.common.utils.DataSchema;
import org.testng.annotations.Test;


public class RowDataBlockTest extends BaseDataBlockContract {
  @Override
  protected BaseDataBlock deserialize(ByteBuffer byteBuffer, int versionType)
      throws IOException {
    return new RowDataBlock(byteBuffer);
  }

  @Test
  public void emptyDataBlockCorrectness()
      throws IOException {
    DataSchema dataSchema = new DataSchema(new String[0], new DataSchema.ColumnDataType[0]);
    testSerdeCorrectness(new RowDataBlock(0, dataSchema, new String[0], new byte[0], new byte[0]));
  }
}