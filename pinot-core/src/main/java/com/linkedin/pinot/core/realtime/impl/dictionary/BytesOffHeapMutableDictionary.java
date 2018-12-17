/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.utils.primitive.ByteArray;
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

  private ByteArray _min = null;
  private ByteArray _max = null;

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
  public int indexOf(Object rawValue) {
    assert rawValue instanceof byte[];
    byte[] bytes = (byte[]) rawValue;
    return getDictId(new ByteArray(bytes), bytes);
  }

  @Override
  public byte[] get(int dictId) {
    return _byteStore.get(dictId);
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return get(dictId);
  }

  @Override
  public void doClose() throws IOException {
    _byteStore.close();
  }

  @Override
  protected void setRawValueAt(int dictId, Object rawValue, byte[] serializedValue) {
    _byteStore.add(serializedValue);
  }

  @Override
  public void index(@Nonnull Object rawValue) {
    assert rawValue instanceof byte[];
    byte[] bytes = (byte[]) rawValue;
    ByteArray byteArray = new ByteArray(bytes);
    indexValue(byteArray, bytes);
    updateMinMax(byteArray);
  }

  @Override
  public boolean inRange(@Nonnull String lower, @Nonnull String upper, int dictIdToCompare, boolean includeLower,
      boolean includeUpper) {
    throw new UnsupportedOperationException("In-range not supported for Bytes data type.");
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
  public ByteArray[] getSortedValues() {
    int numValues = length();
    ByteArray[] sortedValues = new ByteArray[numValues];

    for (int i = 0; i < numValues; i++) {
      sortedValues[i] = getInternal(i);
    }

    Arrays.sort(sortedValues);
    return sortedValues;
  }

  private ByteArray getInternal(int dictId) {
    return new ByteArray(_byteStore.get(dictId));
  }

  protected boolean equalsValueAt(int dictId, Object value, byte[] serializedValue) {
    return _byteStore.equalsValueAt(serializedValue, dictId);
  }

  private void updateMinMax(ByteArray value) {
    if (_min == null) {
      _min = value;
      _max = value;
    } else {
      if (value.compareTo(_min) < 0) {
        _min = value;
      } else if (value.compareTo(_max) > 0) {
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
