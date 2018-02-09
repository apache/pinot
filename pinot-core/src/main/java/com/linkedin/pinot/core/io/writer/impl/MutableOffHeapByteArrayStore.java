/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.io.writer.impl;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @class OffHeapMutableByteArrayStore
 *
 * An off-heap byte array store that provides APIs to add byte array (value), retrieve a value, and compare value at
 * an index. No verification is made as to whether the value added already exists or not.
 * Empty byte arrays are supported.
 *
 * @note The class is thread-safe for single writer and multiple readers.
 *
 * This class has a list of OffHeapMutableByteArrayStore.Buffer objects. As Buffer objects get filled, new Buffer objects
 * are added to the list. New Buffers objects have twice the capacity of the previous Buffer
 *
 * Within a Buffer object byte arrays (values) are stored as below:
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
public class MutableOffHeapByteArrayStore implements Closeable {
  private static final int INT_SIZE = V1Constants.Numbers.INTEGER_SIZE;
  private static final Logger LOGGER = LoggerFactory.getLogger(MutableOffHeapByteArrayStore.class);

  private static class Buffer implements Closeable {

    private final PinotDataBuffer _pinotDataBuffer;
    private final ByteBuffer _byteBuffer;
    private final int _startIndex;
    private final long _size;

    private int _numValues = 0;
    private int _availEndOffset;  // Exclusive

    private Buffer(long size, int startIndex, PinotDataBufferMemoryManager memoryManager, String allocationContext) {
      if (size >= Integer.MAX_VALUE) {
        size = Integer.MAX_VALUE - 1;
      }
      LOGGER.info("Allocating byte array store buffer of size {} for: {}", size, allocationContext);
      _pinotDataBuffer = memoryManager.allocate(size, allocationContext);
      _pinotDataBuffer.order(ByteOrder.nativeOrder());
      _byteBuffer = _pinotDataBuffer.toDirectByteBuffer(0, (int) size);
      _startIndex = startIndex;
      _availEndOffset = _byteBuffer.capacity();
      _size = size;
    }

    private int add(byte[] value) {
      int startOffset = _availEndOffset - value.length;
      if (startOffset < (_numValues + 1) * INT_SIZE) {
        // full
        return -1;
      }
      for (int i = 0, j = startOffset; i < value.length; i++, j++) {
        _byteBuffer.put(j, value[i]);
      }
      _byteBuffer.putInt(_numValues * INT_SIZE, startOffset);
      _availEndOffset = startOffset;
      return _numValues++;
    }

    private boolean equalsValueAt(byte[] value, int index) {
      int startOffset = _byteBuffer.getInt(index * INT_SIZE);
      int endOffset = _byteBuffer.capacity();
      if (index > 0) {
        endOffset = _byteBuffer.getInt((index - 1) * INT_SIZE);
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

    private byte[] get(final int index) {
      int startOffset = _byteBuffer.getInt(index * INT_SIZE);
      int endOffset = _byteBuffer.capacity();
      if (index > 0) {
        endOffset = _byteBuffer.getInt((index - 1) * INT_SIZE);
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

    private long getSize() {
      return _size;
    }

    private int getStartIndex() {
      return _startIndex;
    }
  }

  private volatile List<Buffer> _buffers = new LinkedList<>();
  private int _numElements = 0;
  private volatile Buffer _currentBuffer;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final String _allocationContext;
  private long _totalStringSize = 0;
  private final int _startSize;

  @VisibleForTesting
  public int getStartSize() {
    return _startSize;
  }

  public MutableOffHeapByteArrayStore(PinotDataBufferMemoryManager memoryManager, String allocationContext, int numArrays,
      int avgArrayLen) {
    _memoryManager = memoryManager;
    _allocationContext = allocationContext;
    _startSize = numArrays * (avgArrayLen + 4); // For each array, we store the array and its startoffset (4 bytes)
    expand(_startSize, 0L);
  }

  /**
   * Expand the buffer list to add a new buffer, allocating a buffer that can definitely fit
   * the new value.
   *
   * @param suggestedSize is the size of the new buffer to be allocated
   * @param minSize is the new value that must fit into the new buffer.
   * @return Expanded buffer
   */
  @SuppressWarnings("Duplicates")
  private Buffer expand(long suggestedSize, long minSize) {
    Buffer buffer = new Buffer(Math.max(suggestedSize, minSize), _numElements, _memoryManager, _allocationContext);
    List<Buffer> newList = new LinkedList<>();
    for (Buffer b : _buffers) {
      newList.add(b);
    }
    newList.add(buffer);
    _buffers = newList;
    _currentBuffer = buffer;
    return buffer;
  }

  private Buffer expand(long sizeOfNewValue) {
    return expand(_currentBuffer.getSize() * 2, sizeOfNewValue + INT_SIZE);
  }

  // Returns a byte array, given an index
  public byte[] get(int index) {
    List<Buffer> bufList = _buffers;
    for (int x = bufList.size() - 1; x >= 0; x--) {
      Buffer buffer = bufList.get(x);
      if (index >= buffer.getStartIndex()) {
        return buffer.get(index - buffer.getStartIndex());
      }
    }
    // Assumed that we will never ask for an index that does not exist.
    throw new RuntimeException("dictionary ID '" + index + "' too low");
  }

  // Adds a byte array and returns the index. No verification is made as to whether the byte array already exists or not
  public int add(byte[] value) {
    _totalStringSize += value.length;
    Buffer buffer = _currentBuffer;
    int index = buffer.add(value);
    if (index < 0) {
      buffer = expand(value.length);
      index = buffer.add(value);
    }
    _numElements++;
    return index + buffer.getStartIndex();
  }

  public boolean equalsValueAt(byte[] value, int index) {
    List<Buffer> bufList = _buffers;
    for (int x = bufList.size() - 1; x >= 0; x--) {
      Buffer buffer = bufList.get(x);
      if (index >= buffer.getStartIndex()) {
        return buffer.equalsValueAt(value, index - buffer.getStartIndex());
      }
    }
    throw new RuntimeException("dictionary ID '" + index + "' too low");
  }

  @Override
  public void close() throws IOException {
    _numElements = 0;
    for (Buffer buffer : _buffers) {
      buffer.close();
    }
    _buffers.clear();
  }

  public long getTotalOffHeapMemUsed() {
    long ret = 0;
    for (Buffer buffer : _buffers) {
      ret += buffer.getSize();
    }
    return ret;
  }

  public long getAvgValueSize() {
    if (_numElements > 0) {
      return _totalStringSize / _numElements;
    }
    return 0;
  }
}
