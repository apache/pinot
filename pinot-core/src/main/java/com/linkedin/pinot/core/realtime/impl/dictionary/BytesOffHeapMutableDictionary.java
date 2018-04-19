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

package com.linkedin.pinot.core.realtime.impl.dictionary;

import com.linkedin.pinot.common.utils.primitive.Bytes;
import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.writer.impl.MutableOffHeapByteArrayStore;
import java.io.IOException;
import java.util.Arrays;
import javax.annotation.Nonnull;


/**
 * OffHeap mutable dictionary for Bytes data type.
 */
public class BytesOffHeapMutableDictionary extends BaseOffHeapMutableDictionary {

  private final MutableOffHeapByteArrayStore _byteStore;
  private Bytes _min = null;
  private Bytes _max = null;

  /**
   * Constructor the class.
   *
   * @param estimatedCardinality Estimated cardinality for the column.
   * @param maxOverflowHashSize Max size for in-memory hash.
   * @param memoryManager Memory manager
   * @param allocationContext Context for allocation
   * @param avgLength Estimated average Length of entry
   */
  public BytesOffHeapMutableDictionary(int estimatedCardinality, int maxOverflowHashSize,
      PinotDataBufferMemoryManager memoryManager, String allocationContext, int avgLength) {
    super(estimatedCardinality, maxOverflowHashSize, memoryManager, allocationContext);
    _byteStore = new MutableOffHeapByteArrayStore(memoryManager, allocationContext, estimatedCardinality, avgLength);
  }

  @Override
  public void doClose()
      throws IOException {
    _byteStore.close();
  }

  @Override
  protected void setRawValueAt(int dictId, Object rawValue, byte[] serializedValue) {
    _byteStore.add(serializedValue);
  }

  @Override
  public Object get(int dictionaryId) {
    return _byteStore.get(dictionaryId);
  }

  @Override
  public void index(@Nonnull Object rawValue) {
    if (rawValue instanceof Bytes) {
      // Single value
      Bytes bytes = (Bytes) rawValue;
      indexValue(rawValue, bytes.getBytes());
      updateMinMax(bytes);
    } else if (rawValue instanceof Object[]) {
      // Multi value
      Object[] values = (Object[]) rawValue;
      for (Object value : values) {
        Bytes bytesValue = ((Bytes) value);
        indexValue(value, bytesValue.getBytes());
        updateMinMax(bytesValue);
      }
    } else {
      throw new IllegalArgumentException(
          "Illegal argument type for BytesOffHeapMutableDictionary: " + rawValue.getClass().getName());
    }
  }

  @Override
  public boolean inRange(@Nonnull String lower, @Nonnull String upper, int dictIdToCompare, boolean includeLower,
      boolean includeUpper) {
    throw new UnsupportedOperationException("In-range not supported for Bytes data type.");
  }

  @Override
  public int indexOf(Object rawValue) {
    byte[] value;
    if (rawValue instanceof Bytes) {
      value = ((Bytes) rawValue).getBytes();
    } else if (rawValue instanceof byte[]) {
      value = (byte[]) rawValue;
    } else {
      throw new IllegalArgumentException(
          "Illegal data type for BytesOffHeapMutableDictionary: " + rawValue.getClass().getSimpleName());
    }
    return getDictId(rawValue, value);
  }

  @Nonnull
  @Override
  public Object getMinVal() {
    return _min;
  }

  @Nonnull
  @Override
  public Object getMaxVal() {
    return _max;
  }

  @Nonnull
  @Override
  public Bytes[] getSortedValues() {
    int numValues = length();
    Bytes[] sortedValues = new Bytes[numValues];

    for (int i = 0; i < numValues; i++) {
      sortedValues[i] = getInternal(i);
    }

    Arrays.sort(sortedValues);
    return sortedValues;
  }

  private Bytes getInternal(int dictId) {
    return new Bytes(_byteStore.get(dictId));
  }

  protected boolean equalsValueAt(int dictId, Object value, byte[] serializedValue) {
    return _byteStore.equalsValueAt(serializedValue, dictId);
  }

  private void updateMinMax(Bytes value) {
    if (_min == null) {
      _min = value;
      _max = value;
    } else {
      if (value.compareTo(_min) < 0) {
        _min = value;
      }
      if (value.compareTo(_max) > 0) {
        _max = value;
      }
    }
  }

  @Override
  public long getTotalOffHeapMemUsed() {
    return super.getTotalOffHeapMemUsed() + _byteStore.getTotalOffHeapMemUsed();
  }

  @Override
  public int getAvgValueSize() {
    return (int) _byteStore.getAvgValueSize();
  }
}
