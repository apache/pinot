package org.apache.pinot.segment.spi.memory;

import org.apache.pinot.segment.spi.memory.chronicle.ChroniclePinotBufferFactory;


public class PinotChronicleByteBufferTest extends PinotDataBufferInstanceTestBase {
  public PinotChronicleByteBufferTest() {
    super(new ChroniclePinotBufferFactory());
  }
}
