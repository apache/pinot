package org.apache.pinot.segment.spi.memory;

import java.io.DataInput;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Similar to Guava's {@link com.google.common.io.LittleEndianDataInputStream), but for Pinot.
 */
public abstract class PinotInputStream extends SeekableInputStream implements DataInput {

  public String readInt4UTF()
      throws IOException {
    int length = readInt();
    if (length == 0) {
      return StringUtils.EMPTY;
    } else {
      byte[] bytes = new byte[length];
      readFully(bytes);
      return new String(bytes, UTF_8);
    }
  }

  public abstract long availableLong();

  @Override
  public int skipBytes(int n) {
    if (n <= 0) {
      return 0;
    }
    int step = Math.min(available(), n);
    seek(getCurrentOffset() + step);
    return step;
  }

  @Override
  public int available() {
    long available = availableLong();
    if (available > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) available;
    }
  }

  @Override
  public void readFully(byte[] b)
      throws IOException {
    readFully(b, 0, b.length);
  }
}
