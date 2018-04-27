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

import com.linkedin.pinot.core.io.readerwriter.PinotDataBufferMemoryManager;
import com.linkedin.pinot.core.io.readerwriter.impl.FixedByteSingleColumnSingleValueReaderWriter;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import java.io.IOException;
import java.util.Arrays;
import javax.annotation.Nonnull;


public class IntOffHeapMutableDictionary extends BaseOffHeapMutableDictionary {
  private int _min = Integer.MAX_VALUE;
  private int _max = Integer.MIN_VALUE;

  private final FixedByteSingleColumnSingleValueReaderWriter _dictIdToValue;

  public IntOffHeapMutableDictionary(int estimatedCardinality, int maxOverflowSize, PinotDataBufferMemoryManager memoryManager,
      String allocationContext) {
    super(estimatedCardinality, maxOverflowSize, memoryManager, allocationContext);
    final int initialEntryCount = nearestPowerOf2(estimatedCardinality);
    _dictIdToValue = new FixedByteSingleColumnSingleValueReaderWriter(initialEntryCount, V1Constants.Numbers.INTEGER_SIZE,
        memoryManager, allocationContext);
  }

  public Object get(int dictionaryId) {
    return _dictIdToValue.getInt(dictionaryId);
  }

  @Override
  public int indexOf(Object rawValue) {
    if (rawValue instanceof String) {
      return getDictId(Integer.valueOf((String) rawValue), null);
    } else {
      return getDictId(rawValue, null);
    }
  }

  @Override
  public void index(@Nonnull Object rawValue) {
    if (rawValue instanceof Integer) {
      // Single value
      indexValue(rawValue, null);
      updateMinMax((Integer) rawValue);
    } else {
      // Multi value
      Object[] values = (Object[]) rawValue;
      for (Object value : values) {
        indexValue(value, null);
        updateMinMax((Integer) value);
      }
    }
  }

  @SuppressWarnings("Duplicates")
  @Override
  public boolean inRange(@Nonnull String lower, @Nonnull String upper, int dictIdToCompare, boolean includeLower,
      boolean includeUpper) {
    int lowerInt = Integer.parseInt(lower);
    int upperInt = Integer.parseInt(upper);
    int valueToCompare = (Integer) get(dictIdToCompare);

    if (includeLower) {
      if (valueToCompare < lowerInt) {
        return false;
      }
    } else {
      if (valueToCompare <= lowerInt) {
        return false;
      }
    }

    if (includeUpper) {
      if (valueToCompare > upperInt) {
        return false;
      }
    } else {
      if (valueToCompare >= upperInt) {
        return false;
      }
    }

    return true;
  }

  @Nonnull
  @Override
  public Integer getMinVal() {
    return _min;
  }

  @Nonnull
  @Override
  public Integer getMaxVal() {
    return _max;
  }

  @Nonnull
  @Override
  @SuppressWarnings("Duplicates")
  public int[] getSortedValues() {
    int numValues = length();
    int[] sortedValues = new int[numValues];

    for (int i = 0; i < numValues; i++) {
      sortedValues[i] = (Integer) get(i);
    }

    Arrays.sort(sortedValues);
    return sortedValues;
  }

  @Override
  public int getAvgValueSize() {
    return V1Constants.Numbers.INTEGER_SIZE;
  }

  @Override
  protected void setRawValueAt(int dictId, Object value, byte[] serializedValue) {
    _dictIdToValue.setInt(dictId, (Integer) value);
  }

  @Override
  public int getIntValue(int dictId) {
    return (Integer) get(dictId);
  }

  @Override
  public long getLongValue(int dictId) {
    return (Integer) get(dictId);
  }

  @Override
  public float getFloatValue(int dictId) {
    return (Integer) get(dictId);
  }

  @Override
  public double getDoubleValue(int dictId) {
    return (Integer) get(dictId);
  }

  @Override
  public void doClose() throws IOException {
    _dictIdToValue.close();
  }

  private void updateMinMax(int value) {
    if (value < _min) {
      _min = value;
    }
    if (value > _max) {
      _max = value;
    }
  }

  @Override
  public long getTotalOffHeapMemUsed() {
    return super.getTotalOffHeapMemUsed() + V1Constants.Numbers.INTEGER_SIZE * length();
  }
}
