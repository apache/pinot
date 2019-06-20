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
package org.apache.pinot.core.io.writer.impl;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MutableOffHeapByteArrayStore implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutableOffHeapByteArrayStore.class);

  private static class Buffer extends OffHeapByteArrayStore {
    final private int _startIndex;
    public Buffer(long size, int startIndex, PinotDataBufferMemoryManager memoryManager, String allocationContext) {
      super(size, memoryManager, allocationContext);
      _startIndex = startIndex;
    }

    public int getStartIndex() {
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

  public MutableOffHeapByteArrayStore(PinotDataBufferMemoryManager memoryManager, String allocationContext,
      int numArrays, int avgArrayLen) {
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
    return expand(_currentBuffer.getSize() * 2, sizeOfNewValue + Integer.BYTES);
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
  public void close()
      throws IOException {
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
