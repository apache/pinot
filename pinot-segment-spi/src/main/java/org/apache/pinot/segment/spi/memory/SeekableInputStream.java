package org.apache.pinot.segment.spi.memory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;


/**
 * SeekableInputStream is a class with the methods needed by Pinot to read data efficiently.
 * <p>
 * This class is based on Parquet's SeekableInputStream.
 */
public abstract class SeekableInputStream extends InputStream {

  /**
   * Return the current position in the InputStream.
   *
   * @return current position in bytes from the start of the stream
   */
  public abstract long getCurrentOffset();

  /**
   * Seek to a new position in the InputStream.
   *
   * @param newPos the new position to seek to
   * @throws IllegalArgumentException If the new position is negative or greater than the length of the stream
   */
  public abstract void seek(long newPos);

  /**
   * Read {@code buf.remaining()} bytes of data into a {@link ByteBuffer}.
   * <p>
   * This method will copy available bytes into the buffer, reading at most
   * {@code buf.remaining()} bytes. The number of bytes actually copied is
   * returned by the method, or -1 is returned to signal that the end of the
   * underlying stream has been reached.
   *
   * @param buf a byte buffer to fill with data from the stream
   * @return the number of bytes read or -1 if the stream ended. It may be 0 if there are no bytes available in the
   *         stream
   * @throws IOException If the underlying stream throws IOException
   */
  public abstract int read(ByteBuffer buf) throws IOException;
}
