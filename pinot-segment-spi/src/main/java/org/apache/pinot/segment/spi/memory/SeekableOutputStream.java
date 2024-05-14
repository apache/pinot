package org.apache.pinot.segment.spi.memory;

import java.io.OutputStream;


public abstract class SeekableOutputStream extends OutputStream {

  /**
   * Return the current position in the OutputStream.
   *
   * @return current position in bytes from the start of the stream
   */
  public abstract long getCurrentOffset();

  /**
   * Seek to a new position in the OutputStream.
   *
   * @param newPos the new position to seek to
   * @throws IllegalArgumentException If the new position is negative
   */
  public abstract void seek(long newPos);

  /**
   * Moves the current offset, applying the given change.
   * @param change the change to apply to the current offset
   * @throws IllegalArgumentException if the new position is negative
   */
  public void moveCurrentOffset(long change) {
    long newOffset = getCurrentOffset() + change;
    seek(newOffset);
  }
}
