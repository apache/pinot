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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import java.io.IOException;
import java.util.Arrays;
import org.apache.pinot.segment.local.io.readerwriter.PinotDataBufferMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.MutableOffHeapByteArrayStore;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.request.context.predicate.RangePredicate;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;


@SuppressWarnings("Duplicates")
public class BytesOffHeapMutableDictionary extends BaseOffHeapMutableDictionary {
  private final MutableOffHeapByteArrayStore _byteStore;

  private volatile byte[] _min = null;
  private volatile byte[] _max = null;

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
  public int index(Object value) {
    byte[] bytesValue = (byte[]) value;
    updateMinMax(bytesValue);
    return indexValue(new ByteArray(bytesValue), bytesValue);
  }

  @Override
  public int[] index(Object[] values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compare(int dictId1, int dictId2) {
    return ByteArray.compare(getBytesValue(dictId1), getBytesValue(dictId2));
  }

  @Override
  public IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper) {
    int numValues = length();
    if (numValues == 0) {
      return IntSets.EMPTY_SET;
    }
    IntSet dictIds = new IntOpenHashSet();

    int lowerCompareThreshold = includeLower ? 0 : 1;
    int upperCompareThreshold = includeUpper ? 0 : -1;
    if (lower.equals(RangePredicate.UNBOUNDED)) {
      byte[] upperValue = BytesUtils.toBytes(upper);
      for (int dictId = 0; dictId < numValues; dictId++) {
        byte[] value = getBytesValue(dictId);
        if (ByteArray.compare(value, upperValue) <= upperCompareThreshold) {
          dictIds.add(dictId);
        }
      }
    } else if (upper.equals(RangePredicate.UNBOUNDED)) {
      byte[] lowerValue = BytesUtils.toBytes(lower);
      for (int dictId = 0; dictId < numValues; dictId++) {
        byte[] value = getBytesValue(dictId);
        if (ByteArray.compare(value, lowerValue) >= lowerCompareThreshold) {
          dictIds.add(dictId);
        }
      }
    } else {
      byte[] lowerValue = BytesUtils.toBytes(lower);
      byte[] upperValue = BytesUtils.toBytes(upper);
      for (int dictId = 0; dictId < numValues; dictId++) {
        byte[] value = getBytesValue(dictId);
        if (ByteArray.compare(value, lowerValue) >= lowerCompareThreshold
            && ByteArray.compare(value, upperValue) <= upperCompareThreshold) {
          dictIds.add(dictId);
        }
      }
    }
    return dictIds;
  }

  @Override
  public ByteArray getMinVal() {
    return new ByteArray(_min);
  }

  @Override
  public ByteArray getMaxVal() {
    return new ByteArray(_max);
  }

  @Override
  public ByteArray[] getSortedValues() {
    int numValues = length();
    ByteArray[] sortedValues = new ByteArray[numValues];

    for (int dictId = 0; dictId < numValues; dictId++) {
      sortedValues[dictId] = new ByteArray(getBytesValue(dictId));
    }

    Arrays.sort(sortedValues);
    return sortedValues;
  }

  @Override
  public DataType getValueType() {
    return DataType.BYTES;
  }

  @Override
  public int indexOf(String stringValue) {
    byte[] bytesValue = BytesUtils.toBytes(stringValue);
    return getDictId(new ByteArray(bytesValue), bytesValue);
  }

  @Override
  public byte[] get(int dictId) {
    return getBytesValue(dictId);
  }

  @Override
  public Object getInternal(int dictId) {
    return getByteArrayValue(dictId);
  }

  @Override
  public int getIntValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLongValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloatValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDoubleValue(int dictId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getStringValue(int dictId) {
    return BytesUtils.toHexString(getBytesValue(dictId));
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return _byteStore.get(dictId);
  }

  @Override
  protected void setValue(int dictId, Object value, byte[] serializedValue) {
    _byteStore.add(serializedValue);
  }

  @Override
  protected boolean equalsValueAt(int dictId, Object value, byte[] serializedValue) {
    return _byteStore.equalsValueAt(serializedValue, dictId);
  }

  @Override
  public int getAvgValueSize() {
    return (int) _byteStore.getAvgValueSize();
  }

  @Override
  public long getTotalOffHeapMemUsed() {
    return getOffHeapMemUsed() + _byteStore.getTotalOffHeapMemUsed();
  }

  @Override
  public void doClose()
      throws IOException {
    _byteStore.close();
  }

  private void updateMinMax(byte[] value) {
    if (_min == null) {
      _min = value;
      _max = value;
    } else {
      if (ByteArray.compare(value, _min) < 0) {
        _min = value;
      }
      if (ByteArray.compare(value, _max) > 0) {
        _max = value;
      }
    }
  }
}
