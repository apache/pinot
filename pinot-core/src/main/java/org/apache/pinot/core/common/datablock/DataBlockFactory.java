package org.apache.pinot.core.common.datablock;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableImplV4;


public class DataBlockFactory {
  private DataBlockFactory() {
  }

  public static BaseDataBlock getDataBlock(ByteBuffer byteBuffer)
      throws IOException {
    int version = byteBuffer.getInt();
    switch (version) {
      // TODO(walterddr): support returning RowDataBlock and ColumnarDataBlock.
      case DataTableBuilder.VERSION_4:
        return new DataTableImplV4(byteBuffer);
      default:
        throw new UnsupportedOperationException("Unsupported data table version: " + version);
    }
  }

  public static BaseDataBlock getDataBlock(byte[] bytes)
      throws IOException {
    return getDataBlock(ByteBuffer.wrap(bytes));
  }
}
