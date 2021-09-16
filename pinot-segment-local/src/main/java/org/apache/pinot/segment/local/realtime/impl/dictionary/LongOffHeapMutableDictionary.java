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
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.request.context.predicate.RangePredicate;


@SuppressWarnings("Duplicates")
public class LongOffHeapMutableDictionary extends BaseOffHeapMutableDictionary {
  private final FixedByteSVMutableForwardIndex _dictIdToValue;

  private volatile long _min = Long.MAX_VALUE;
  private volatile long _max = Long.MIN_VALUE;

  public LongOffHeapMutableDictionary(int estimatedCardinality, int overflowSize,
      PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    super(estimatedCardinality, overflowSize, memoryManager, allocationContext);
    int initialEntryCount = nearestPowerOf2(estimatedCardinality);
    _dictIdToValue =
        new FixedByteSVMutableForwardIndex(false, DataType.LONG, initialEntryCount, memoryManager, allocationContext);
  }

  @Override
  public int index(Object value) {
    Long longValue = (Long) value;
    updateMinMax(longValue);
    return indexValue(longValue, null);
  }

  @Override
  public int[] index(Object[] values) {
    int numValues = values.length;
    int[] dictIds = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      Long longValue = (Long) values[i];
      updateMinMax(longValue);
      dictIds[i] = indexValue(longValue, null);
    }
    return dictIds;
  }

  @Override
  public int compare(int dictId1, int dictId2) {
    return Long.compare(getLongValue(dictId1), getLongValue(dictId2));
  }

  @Override
  public IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper) {
    int numValues = length();
    if (numValues == 0) {
      return IntSets.EMPTY_SET;
    }
    IntSet dictIds = new IntOpenHashSet();

    if (lower.equals(RangePredicate.UNBOUNDED)) {
      long upperValue = Long.parseLong(upper);
      if (includeUpper) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          long value = getLongValue(dictId);
          if (value <= upperValue) {
            dictIds.add(dictId);
          }
        }
      } else {
        for (int dictId = 0; dictId < numValues; dictId++) {
          long value = getLongValue(dictId);
          if (value < upperValue) {
            dictIds.add(dictId);
          }
        }
      }
    } else if (upper.equals(RangePredicate.UNBOUNDED)) {
      long lowerValue = Long.parseLong(lower);
      if (includeLower) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          long value = getLongValue(dictId);
          if (value >= lowerValue) {
            dictIds.add(dictId);
          }
        }
      } else {
        for (int dictId = 0; dictId < numValues; dictId++) {
          long value = getLongValue(dictId);
          if (value > lowerValue) {
            dictIds.add(dictId);
          }
        }
      }
    } else {
      long lowerValue = Long.parseLong(lower);
      long upperValue = Long.parseLong(upper);
      if (includeLower && includeUpper) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          long value = getLongValue(dictId);
          if (value >= lowerValue && value <= upperValue) {
            dictIds.add(dictId);
          }
        }
      } else if (includeLower) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          long value = getLongValue(dictId);
          if (value >= lowerValue && value < upperValue) {
            dictIds.add(dictId);
          }
        }
      } else if (includeUpper) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          long value = getLongValue(dictId);
          if (value > lowerValue && value <= upperValue) {
            dictIds.add(dictId);
          }
        }
      } else {
        for (int dictId = 0; dictId < numValues; dictId++) {
          long value = getLongValue(dictId);
          if (value > lowerValue && value < upperValue) {
            dictIds.add(dictId);
          }
        }
      }
    }
    return dictIds;
  }

  @Override
  public Long getMinVal() {
    return _min;
  }

  @Override
  public Long getMaxVal() {
    return _max;
  }

  @Override
  public long[] getSortedValues() {
    int numValues = length();
    long[] sortedValues = new long[numValues];

    for (int dictId = 0; dictId < numValues; dictId++) {
      sortedValues[dictId] = getLongValue(dictId);
    }

    Arrays.sort(sortedValues);
    return sortedValues;
  }

  @Override
  public DataType getValueType() {
    return DataType.LONG;
  }

  @Override
  public int indexOf(String stringValue) {
    return getDictId(Long.valueOf(stringValue), null);
  }

  @Override
  public Long get(int dictId) {
    return getLongValue(dictId);
  }

  @Override
  public int getIntValue(int dictId) {
    return (int) getLongValue(dictId);
  }

  @Override
  public long getLongValue(int dictId) {
    return _dictIdToValue.getLong(dictId);
  }

  @Override
  public float getFloatValue(int dictId) {
    return getLongValue(dictId);
  }

  @Override
  public double getDoubleValue(int dictId) {
    return getLongValue(dictId);
  }

  @Override
  public String getStringValue(int dictId) {
    return Long.toString(getLongValue(dictId));
  }

  @Override
  protected void setValue(int dictId, Object value, byte[] serializedValue) {
    _dictIdToValue.setLong(dictId, (Long) value);
  }

  @Override
  protected boolean equalsValueAt(int dictId, Object value, byte[] serializedValue) {
    return getLongValue(dictId) == (Long) value;
  }

  @Override
  public int getAvgValueSize() {
    return Long.BYTES;
  }

  @Override
  public long getTotalOffHeapMemUsed() {
    return getOffHeapMemUsed() + Long.BYTES * (long) length();
  }

  @Override
  public void doClose()
      throws IOException {
    _dictIdToValue.close();
  }

  private void updateMinMax(long value) {
    if (value < _min) {
      _min = value;
    }
    if (value > _max) {
      _max = value;
    }
  }
}
