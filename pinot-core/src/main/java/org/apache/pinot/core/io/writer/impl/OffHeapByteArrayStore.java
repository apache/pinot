package org.apache.pinot.core.io.writer.impl;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @class OffHeapByteArrayStore
 *
 * An off-heap byte array store that provides APIs to add byte array (value), retrieve a value, and compare value at
 * an index. No verification is made as to whether the value added already exists or not.
 * Empty byte arrays are supported.
 *
 * @note The class is thread-safe for single writer and multiple readers.
 *
 * Byte arrays (values) are stored as below:
 *
 *                  __________________________________
 *                  |  start offset of array  1      |
 *                  |  start offset of array  2      |
 *                  |        .....                   |
 *                  |  start offset of array  N      |
 *                  |                                |
 *                  |         UNUSED                 |
 *                  |                                |
 *                  |  Array N .....                 |
 *                  |          .....                 |
 *                  |          .....                 |
 *                  |  Array N-1                     |
 *                  |          .....                 |
 *                  |          .....                 |
 *                  |  Array 0 .....                 |
 *                  |          .....                 |
 *                  |          .....                 |
 *                  |________________________________|
 *
 *
 * We fill the buffer as follows:
 * - The values are added from the bottom, each new value appearing nearer to the top of the buffer, leaving no
 *   room between them. Each value is stored as a sequence of bytes.
 *
 * - The start offsets of the byte arrays are added from the top. Each start offset is stored as an integer, taking 4 bytes.
 *
 * Each time we want to add a new value, we check if we have space to add the length of the value, and the value
 * itself. If we do, then we compute the start offset of the new value as:
 *
 *    new-start-offset = (start offset of prev value added) - (length of this value)
 *
 * The new start offset value is stored in the offset
 *
 *    buffer[numValuesSoFar * 4]
 *
 * and the value itself is stored starting at new-start-offset
 *
 */
public class OffHeapByteArrayStore implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(OffHeapByteArrayStore.class);

  private final PinotDataBuffer _pinotDataBuffer;
  private final ByteBuffer _byteBuffer;
  private final long _size;

  private int _numValues = 0;
  private int _availEndOffset;  // Exclusive

  public OffHeapByteArrayStore(long size, PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    if (size >= Integer.MAX_VALUE) {
      size = Integer.MAX_VALUE - 1;
    }
    LOGGER.info("Allocating byte array store buffer of size {} for: {}", size, allocationContext);
    _pinotDataBuffer = memoryManager.allocate(size, allocationContext);
    _byteBuffer = _pinotDataBuffer.toDirectByteBuffer(0, (int) size);
    _availEndOffset = _byteBuffer.capacity();
    _size = size;
  }

  // TODO Re-factor this constructor (and tests) accordingly depending on usage to implement VarLengthValueReaderWriter
  public OffHeapByteArrayStore(int numValues, int totalValuesLen, PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    this((long)numValues + Integer.BYTES + totalValuesLen, memoryManager, allocationContext);
  }

  // TODO Add unit tests for this code path
  public OffHeapByteArrayStore(PinotDataBuffer pinotDataBuffer) {
    _pinotDataBuffer = pinotDataBuffer;
    _byteBuffer = _pinotDataBuffer.toDirectByteBuffer(0, (int)pinotDataBuffer.size());
    _availEndOffset = _byteBuffer.capacity();
    _size = pinotDataBuffer.size();
  }

  /**
   * Adds a byte array to the store
   * @param value is th byte array to add.
   * @return the index to be used to access the value, -1 if there is no room in the store to add the value.
   */

  public int add(byte[] value) {
    int startOffset = _availEndOffset - value.length;
    if (startOffset < (_numValues + 1) * Integer.BYTES) {
      // full
      return -1;
    }
    for (int i = 0, j = startOffset; i < value.length; i++, j++) {
      _byteBuffer.put(j, value[i]);
    }
    _byteBuffer.putInt(_numValues * Integer.BYTES, startOffset);
    _availEndOffset = startOffset;
    return _numValues++;
  }

  /**
   *  Checks if the byte array at a given index is the same as the one passed in
   *  It is assumed that the index passed is in a valid one. Invalid index can
   *  result in memory access or other exceptions.
   *
   * @param value Value to compare
   * @param index Index at which to compare.
   * @return
   */
  public boolean equalsValueAt(byte[] value, int index) {
    int startOffset = _byteBuffer.getInt(index * Integer.BYTES);
    int endOffset = _byteBuffer.capacity();
    if (index > 0) {
      endOffset = _byteBuffer.getInt((index - 1) * Integer.BYTES);
    }
    if ((endOffset - startOffset) != value.length) {
      return false;
    }
    for (int i = 0, j = startOffset; i < value.length; i++, j++) {
      if (value[i] != _byteBuffer.get(j)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the value stored at the specified index.
   * It is assumed that the index is a valid one. Invalid index can result in
   * memory access errors.
   *
   * @param index Index of the value to retrieve from the store.
   * @return
   */
  public byte[] get(final int index) {
    int startOffset = _byteBuffer.getInt(index * Integer.BYTES);
    int endOffset = _byteBuffer.capacity();
    if (index > 0) {
      endOffset = _byteBuffer.getInt((index - 1) * Integer.BYTES);
    }
    byte[] value = new byte[endOffset - startOffset];
    for (int i = 0, j = startOffset; i < value.length; i++, j++) {
      value[i] = _byteBuffer.get(j);
    }
    return value;
  }

  @Override
  public void close()
      throws IOException {
    _pinotDataBuffer.close();
  }

  public long getSize() {
    return _size;
  }
}
