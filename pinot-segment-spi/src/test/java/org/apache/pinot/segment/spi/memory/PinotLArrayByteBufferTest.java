package org.apache.pinot.segment.spi.memory;

import net.openhft.chronicle.core.Jvm;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;


public class PinotLArrayByteBufferTest extends PinotDataBufferInstanceTestBase {
  public PinotLArrayByteBufferTest() {
    super(new LArrayPinotBufferFactory());
  }

  @BeforeClass
  public void abortOnModernJava() {
    if (Jvm.majorVersion() > 11) {
//      throw new SkipException("Skipping LArray tests because they cannot run in Java " + Jvm.majorVersion());
    }
  }
}
