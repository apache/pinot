/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.io.writer.impl;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.pinot.segment.local.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(MutableOffHeapByteArrayStore.class);

  private static class Buffer implements Closeable {
    private final PinotDataBuffer _pinotDataBuffer;
    private final int _startIndex;
    private final int _size;

    private int _numValues = 0;
    private int _availEndOffset; // Exclusive

    private Buffer(int size, int startIndex, PinotDataBufferMemoryManager memoryManager, String allocationContext) {
      LOGGER.info("Allocating byte array store buffer of size {} for: {}", size, allocationContext);
      _pinotDataBuffer = memoryManager.allocate(size, allocationContext);
      _startIndex = startIndex;
      _size = size;
      _availEndOffset = size;
    }

    private int add(byte[] value) {
      int startOffset = _availEndOffset - value.length;
      if (startOffset < (_numValues + 1) * Integer.BYTES) {
        // full
        return -1;
      }
      _pinotDataBuffer.readFrom(startOffset, value);
      _pinotDataBuffer.putInt(_numValues * Integer.BYTES, startOffset);
      _availEndOffset = startOffset;
      return _numValues++;
    }

    private boolean equalsValueAt(byte[] value, int index) {
      int startOffset = _pinotDataBuffer.getInt(index * Integer.BYTES);
      int endOffset;
      if (index != 0) {
        endOffset = _pinotDataBuffer.getInt((index - 1) * Integer.BYTES);
      } else {
        endOffset = _size;
      }
      if ((endOffset - startOffset) != value.length) {
        return false;
      }
      for (int i = 0, j = startOffset; i < value.length; i++, j++) {
        if (value[i] != _pinotDataBuffer.getByte(j)) {
          return false;
        }
      }
      return true;
    }

    private byte[] get(int index) {
      int startOffset = _pinotDataBuffer.getInt(index * Integer.BYTES);
      int endOffset;
      if (index != 0) {
        endOffset = _pinotDataBuffer.getInt((index - 1) * Integer.BYTES);
      } else {
        endOffset = _size;
      }
      byte[] value = new byte[endOffset - startOffset];
      _pinotDataBuffer.copyTo(startOffset, value);
      return value;
    }

    private int getSize() {
      return _size;
    }

    private int getStartIndex() {
      return _startIndex;
    }

    @Override
    public void close() throws IOException {
      // NOTE: DO NOT close the PinotDataBuffer here because it is tracked in the PinotDataBufferMemoryManager.
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

  public MutableOffHeapByteArrayStore(PinotDataBufferMemoryManager memoryManager, String allocationContext,
      int numArrays, int avgArrayLen) {
    _memoryManager = memoryManager;
    _allocationContext = allocationContext;
    _startSize = numArrays * (avgArrayLen + 4); // For each array, we store the array and its startoffset (4 bytes)
    expand(_startSize);
  }

  /**
   * Expand the buffer list to add a new buffer, allocating a buffer that can definitely fit
   * the new value.
   *
   * @param size Size of the expanded buffer
   * @return Expanded buffer
   */
  private Buffer expand(int size) {
    Buffer buffer = new Buffer(size, _numElements, _memoryManager, _allocationContext);
    List<Buffer> newList = new LinkedList<>(_buffers);
    newList.add(buffer);
    _buffers = newList;
    _currentBuffer = buffer;
    return buffer;
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
    int valueLength = value.length;
    Buffer buffer = _currentBuffer;
    int index = buffer.add(value);
    if (index < 0) {
      // Need to expand the buffer
      int currentBufferSize = buffer.getSize();
      if ((currentBufferSize << 1) >= 0) {
        // The expanded buffer size should be enough for the current value
        buffer = expand(Math.max(currentBufferSize << 1, valueLength + Integer.BYTES));
      } else {
        // Int overflow
        buffer = expand(Integer.MAX_VALUE);
      }
      index = buffer.add(value);
    }
    _totalStringSize += valueLength;
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
    for (Buffer buffer : _buffers) {
      buffer.close();
    }
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
