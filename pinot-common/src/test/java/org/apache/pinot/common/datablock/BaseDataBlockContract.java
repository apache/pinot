package org.apache.pinot.common.datablock;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.testng.Assert.*;


public abstract class BaseDataBlockContract {

  protected abstract BaseDataBlock deserialize(ByteBuffer byteBuffer, int versionType)
      throws IOException;

  public void testSerdeCorrectness(BaseDataBlock dataBlock)
      throws IOException {
    byte[] bytes = dataBlock.toBytes();
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    int versionType = DataBlockUtils.readVersionType(byteBuffer);
    BaseDataBlock deserialize = deserialize(byteBuffer, versionType);

    assertEquals(byteBuffer.position(), bytes.length, "Buffer position should be at the end of the buffer");
    assertEquals(deserialize, dataBlock, "Deserialized data block should be the same as the original data block");
  }
}