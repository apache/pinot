package org.apache.pinot.query.runtime.operator;

import java.util.List;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


public class OperatorTestUtil {
  public static final DataSchema TEST_DATA_SCHEMA = new DataSchema(new String[]{"foo", "bar"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

  public static TransferableBlock getEndOfStreamRowBlock() {
      return getEndOfStreamRowBlockWithSchema(TEST_DATA_SCHEMA);
  }

  public static TransferableBlock getEndOfStreamRowBlockWithSchema(DataSchema schema){
    return new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock(schema));
  }

  public static TransferableBlock getRowDataBlock(List<Object[]> rows){
    return getRowDataBlockWithSchema(rows, TEST_DATA_SCHEMA);
  }

  public static TransferableBlock getRowDataBlockWithSchema(List<Object[]> rows, DataSchema schema){
    return new TransferableBlock(rows, schema, BaseDataBlock.Type.ROW);
  }
}
