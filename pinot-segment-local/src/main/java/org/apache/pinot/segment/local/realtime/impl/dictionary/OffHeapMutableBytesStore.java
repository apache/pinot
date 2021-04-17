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
package org.apache.pinot.segment.local.realtime.impl.dictionary;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.segment.local.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;


/**
 * Off-heap variable length mutable bytes store.
 * <p>The class is thread-safe for single writer and multiple readers.
 * <p>There are two sets of buffers allocated for the store:
 * <ul>
 *   <li>
 *     Offset buffer: stores the value end offsets. The end offset here is the global offset for all the value buffers
 *     instead of the offset inside one value buffer (e.g. offset 9 in the 5th value buffer will have global offset of
 *     4 * VALUE_BUFFER_SIZE + 9). New offset buffer is allocated when the previous buffer is filled up. The first entry
 *     of each offset buffer stores the end offset of the last value (or 0 for the first offset buffer) so that we can
 *     always get the previous and current value's end offset from the same buffer. Note that we use int type for the
 *     offset, so each bytes store can hold at most 2^31 = 2GB total size of values.
 *   </li>
 *   <li>
 *     Value buffer: stores the values. For performance concern, to avoid reading one value from multiple buffers, each
 *     value is stored in one value buffer (no cross-buffer value allowed), and has a size limit of the
 *     VALUE_BUFFER_SIZE (1MB). New value buffer is allocated when the previous buffer cannot hold the new added value.
 *   </li>
 * </ul>
 * <p>The maximum capacity for the bytes store is 2^24 ~= 17M values and 2^31 = 2GB total size of values.
 */
@ThreadSafe
public class OffHeapMutableBytesStore implements Closeable {
  private static final byte[] EMPTY_BYTES = new byte[0];

  // Each offset buffer holds 8_192 end offsets and one extra end offset for the last value in the previous buffer (or 0
  // for the first buffer)
  // OFFSET_BUFFER_SIZE = (8_192 + 1) * 4 = 32_772
  private static final int OFFSET_BUFFER_SHIFT_OFFSET = 13;
  private static final int OFFSET_BUFFER_SIZE = ((1 << OFFSET_BUFFER_SHIFT_OFFSET) + 1) << 2;
  private static final int OFFSET_BUFFER_MASK = (1 << OFFSET_BUFFER_SHIFT_OFFSET) - 1;

  // Each value buffer holds up to 1_048_576 bytes, which is also the size limit for each value
  // VALUE_BUFFER_SIZE = 1_048_576
  private static final int VALUE_BUFFER_SHIFT_OFFSET = 20;
  private static final int VALUE_BUFFER_SIZE = 1 << VALUE_BUFFER_SHIFT_OFFSET;
  private static final int VALUE_BUFFER_MASK = (1 << VALUE_BUFFER_SHIFT_OFFSET) - 1;

  // With at most 2_048 buffers, we can store about 17M values of total size up to 2GB
  // MAX_NUM_BUFFERS = 2_048
  private static final int MAX_NUM_BUFFERS = 1 << 11;

  private final AtomicReferenceArray<PinotDataBuffer> _offsetBuffers = new AtomicReferenceArray<>(MAX_NUM_BUFFERS);
  private final AtomicReferenceArray<PinotDataBuffer> _valueBuffers = new AtomicReferenceArray<>(MAX_NUM_BUFFERS);
  private final PinotDataBufferMemoryManager _memoryManager;
  private final String _allocationContext;

  // Global end offset of the previous added value
  private int _previousValueEndOffset;
  // Number of values added
  private transient int _numValues;
  // Total size of the OffHeap buffers (both offset buffers and value buffers)
  private transient int _totalBufferSize;

  public OffHeapMutableBytesStore(PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    _memoryManager = memoryManager;
    _allocationContext = allocationContext;
  }

  /**
   * Adds a value into the bytes store. The size of the given value should be smaller or equal to the VALUE_BUFFER_SIZE
   * (1MB).
   */
  public int add(byte[] value) {
    int offsetBufferIndex = _numValues >>> OFFSET_BUFFER_SHIFT_OFFSET;
    int offsetIndex = _numValues & OFFSET_BUFFER_MASK;

    PinotDataBuffer offsetBuffer;
    // If this is the first entry in the offset buffer, allocate a new buffer and store the end offset of the previous
    // value
    if (offsetIndex == 0) {
      offsetBuffer = _memoryManager.allocate(OFFSET_BUFFER_SIZE, _allocationContext);
      offsetBuffer.putInt(0, _previousValueEndOffset);
      _offsetBuffers.set(offsetBufferIndex, offsetBuffer);
      _totalBufferSize += OFFSET_BUFFER_SIZE;
    } else {
      offsetBuffer = _offsetBuffers.get(offsetBufferIndex);
    }

    int valueLength = value.length;
    if (valueLength == 0) {
      offsetBuffer.putInt((offsetIndex + 1) << 2, _previousValueEndOffset);
      return _numValues++;
    }

    int valueBufferIndex = (_previousValueEndOffset + valueLength - 1) >>> VALUE_BUFFER_SHIFT_OFFSET;
    PinotDataBuffer valueBuffer;
    int valueStartOffset;
    // If the current value buffer does not have enough space, allocate a new buffer to store the value
    if ((_previousValueEndOffset - 1) >>> VALUE_BUFFER_SHIFT_OFFSET != valueBufferIndex) {
      valueBuffer = _memoryManager.allocate(VALUE_BUFFER_SIZE, _allocationContext);
      _valueBuffers.set(valueBufferIndex, valueBuffer);
      _totalBufferSize += VALUE_BUFFER_SIZE;
      valueStartOffset = valueBufferIndex << VALUE_BUFFER_SHIFT_OFFSET;
    } else {
      valueBuffer = _valueBuffers.get(valueBufferIndex);
      valueStartOffset = _previousValueEndOffset;
    }

    int valueEndOffset = valueStartOffset + valueLength;
    offsetBuffer.putInt((offsetIndex + 1) << 2, valueEndOffset);
    valueBuffer.readFrom(valueStartOffset & VALUE_BUFFER_MASK, value);
    _previousValueEndOffset = valueEndOffset;

    return _numValues++;
  }

  /**
   * Returns the value at the given index. The given index should be smaller than the number of values already added.
   */
  @SuppressWarnings("Duplicates")
  public byte[] get(int index) {
    assert index < _numValues;

    int offsetBufferIndex = index >>> OFFSET_BUFFER_SHIFT_OFFSET;
    PinotDataBuffer offsetBuffer = _offsetBuffers.get(offsetBufferIndex);
    int offsetIndex = index & OFFSET_BUFFER_MASK;
    int previousValueEndOffset = offsetBuffer.getInt(offsetIndex << 2);
    int valueEndOffset = offsetBuffer.getInt((offsetIndex + 1) << 2);

    if (previousValueEndOffset == valueEndOffset) {
      return EMPTY_BYTES;
    }

    int valueBufferIndex = (valueEndOffset - 1) >>> VALUE_BUFFER_SHIFT_OFFSET;
    int startOffsetInValueBuffer;
    int valueLength;
    if ((previousValueEndOffset - 1) >>> VALUE_BUFFER_SHIFT_OFFSET != valueBufferIndex) {
      // The first value in the value buffer
      startOffsetInValueBuffer = 0;
      valueLength = valueEndOffset & VALUE_BUFFER_MASK;
    } else {
      // Not the first value in the value buffer
      startOffsetInValueBuffer = previousValueEndOffset & VALUE_BUFFER_MASK;
      valueLength = valueEndOffset - previousValueEndOffset;
    }

    byte[] value = new byte[valueLength];
    _valueBuffers.get(valueBufferIndex).copyTo(startOffsetInValueBuffer, value);
    return value;
  }

  /**
   * Returns whether the value at the given index equals the given value. The given index should be smaller than the
   * number of values already added.
   * <p>This method should be preferable when checking whether a value equals the value at a specific index in the bytes
   * store compared to calling {@link #get(int)} and then compare the values because if the value length does not match,
   * the value fetch can be skipped.
   */
  @SuppressWarnings("Duplicates")
  public boolean equalsValueAt(int index, byte[] valueToCompare) {
    assert index < _numValues;

    int offsetBufferIndex = index >>> OFFSET_BUFFER_SHIFT_OFFSET;
    PinotDataBuffer offsetBuffer = _offsetBuffers.get(offsetBufferIndex);
    int offsetIndex = index & OFFSET_BUFFER_MASK;
    int previousValueEndOffset = offsetBuffer.getInt(offsetIndex << 2);
    int valueEndOffset = offsetBuffer.getInt((offsetIndex + 1) << 2);

    int inputValueLength = valueToCompare.length;
    if (previousValueEndOffset == valueEndOffset) {
      return inputValueLength == 0;
    }

    // Check value length first
    int valueBufferIndex = (valueEndOffset - 1) >>> VALUE_BUFFER_SHIFT_OFFSET;
    int startOffsetInValueBuffer;
    if ((previousValueEndOffset - 1) >>> VALUE_BUFFER_SHIFT_OFFSET != valueBufferIndex) {
      // The first value in the value buffer
      if ((valueEndOffset & VALUE_BUFFER_MASK) != inputValueLength) {
        return false;
      }
      startOffsetInValueBuffer = 0;
    } else {
      // Not the first value in the value buffer
      if (valueEndOffset - previousValueEndOffset != inputValueLength) {
        return false;
      }
      startOffsetInValueBuffer = previousValueEndOffset & VALUE_BUFFER_MASK;
    }

    // Value length matches, check value
    PinotDataBuffer valueBuffer = _valueBuffers.get(valueBufferIndex);
    if (inputValueLength <= PinotDataBuffer.BULK_BYTES_PROCESSING_THRESHOLD) {
      for (int i = 0; i < inputValueLength; i++) {
        if (valueToCompare[i] != valueBuffer.getByte(startOffsetInValueBuffer + i)) {
          return false;
        }
      }
      return true;
    } else {
      byte[] value = new byte[inputValueLength];
      valueBuffer.copyTo(startOffsetInValueBuffer, value);
      for (int i = 0; i < inputValueLength; i++) {
        if (valueToCompare[i] != value[i]) {
          return false;
        }
      }
      return true;
    }
  }

  public int getNumValues() {
    return _numValues;
  }

  public int getTotalBufferSize() {
    return _totalBufferSize;
  }

  @Override
  public void close() throws IOException {
    for (int i = 0; i < MAX_NUM_BUFFERS; i++) {
      PinotDataBuffer offsetBuffer = _offsetBuffers.get(i);
      if (offsetBuffer != null) {
        offsetBuffer.close();
      } else {
        break;
      }
    }
    for (int i = 0; i < MAX_NUM_BUFFERS; i++) {
      PinotDataBuffer valueBuffer = _valueBuffers.get(i);
      if (valueBuffer != null) {
        valueBuffer.close();
      } else {
        break;
      }
    }
  }
}
