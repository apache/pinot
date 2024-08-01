package org.apache.pinot.plugin.inputformat.avro.datahub.util;

import java.io.ByteArrayInputStream;

public final class MutableByteArrayInputStream extends ByteArrayInputStream {
  public MutableByteArrayInputStream() {
    super(new byte[0]);
  }

  public void setBuffer(byte[] buf) {
    this.buf = buf;
    this.pos = 0;
    this.count = buf.length;
  }
}
