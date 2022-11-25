package org.apache.pinot.segment.spi.memory;

public class PinotByteBufferTest extends PinotDataBufferInstanceTestBase {
  public PinotByteBufferTest() {
    super(new ByteBufferPinotBufferFactory());
  }
}
