package org.apache.pinot.core.data.manager.offline;

import java.nio.ByteBuffer;
import org.apache.pinot.spi.data.FieldSpec;


public class FixedWidthJoinKey {

  private final byte[] _keys;
  private ByteBuffer _buffer;

  public FixedWidthJoinKey(FieldSpec.DataType... types) {
    int size = 0;
    for (FieldSpec.DataType type : types) {
      size += type.getStoredType().size();
    }
    _keys = new byte[size];
    _buffer = ByteBuffer.wrap(_keys);
  }
}
