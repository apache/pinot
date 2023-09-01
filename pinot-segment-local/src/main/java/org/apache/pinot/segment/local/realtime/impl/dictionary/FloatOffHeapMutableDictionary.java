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
import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec.DataType;


@SuppressWarnings("Duplicates")
public class FloatOffHeapMutableDictionary extends BaseOffHeapMutableDictionary {
  private final FixedByteSVMutableForwardIndex _dictIdToValue;

  private volatile float _min = Float.MAX_VALUE;
  private volatile float _max = Float.MIN_VALUE;

  public FloatOffHeapMutableDictionary(int estimatedCardinality, int maxOverflowSize,
      PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    super(estimatedCardinality, maxOverflowSize, memoryManager, allocationContext);
    int initialEntryCount = nearestPowerOf2(estimatedCardinality);
    _dictIdToValue =
        new FixedByteSVMutableForwardIndex(false, DataType.FLOAT, initialEntryCount, memoryManager, allocationContext);
  }

  @Override
  public int index(Object value) {
    Float floatValue = (Float) value;
    updateMinMax(floatValue);
    return indexValue(floatValue, null);
  }

  @Override
  public int[] index(Object[] values) {
    int numValues = values.length;
    int[] dictIds = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      Float floatValue = (Float) values[i];
      updateMinMax(floatValue);
      dictIds[i] = indexValue(floatValue, null);
    }
    return dictIds;
  }

  @Override
  public int compare(int dictId1, int dictId2) {
    return Float.compare(getFloatValue(dictId1), getFloatValue(dictId2));
  }

  @Override
  public IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper) {
    int numValues = length();
    if (numValues == 0) {
      return IntSets.EMPTY_SET;
    }
    IntSet dictIds = new IntOpenHashSet();

    if (lower.equals(RangePredicate.UNBOUNDED)) {
      float upperValue = Float.parseFloat(upper);
      if (includeUpper) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          float value = getFloatValue(dictId);
          if (value <= upperValue) {
            dictIds.add(dictId);
          }
        }
      } else {
        for (int dictId = 0; dictId < numValues; dictId++) {
          float value = getFloatValue(dictId);
          if (value < upperValue) {
            dictIds.add(dictId);
          }
        }
      }
    } else if (upper.equals(RangePredicate.UNBOUNDED)) {
      float lowerValue = Float.parseFloat(lower);
      if (includeLower) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          float value = getFloatValue(dictId);
          if (value >= lowerValue) {
            dictIds.add(dictId);
          }
        }
      } else {
        for (int dictId = 0; dictId < numValues; dictId++) {
          float value = getFloatValue(dictId);
          if (value > lowerValue) {
            dictIds.add(dictId);
          }
        }
      }
    } else {
      float lowerValue = Float.parseFloat(lower);
      float upperValue = Float.parseFloat(upper);
      if (includeLower && includeUpper) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          float value = getFloatValue(dictId);
          if (value >= lowerValue && value <= upperValue) {
            dictIds.add(dictId);
          }
        }
      } else if (includeLower) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          float value = getFloatValue(dictId);
          if (value >= lowerValue && value < upperValue) {
            dictIds.add(dictId);
          }
        }
      } else if (includeUpper) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          float value = getFloatValue(dictId);
          if (value > lowerValue && value <= upperValue) {
            dictIds.add(dictId);
          }
        }
      } else {
        for (int dictId = 0; dictId < numValues; dictId++) {
          float value = getFloatValue(dictId);
          if (value > lowerValue && value < upperValue) {
            dictIds.add(dictId);
          }
        }
      }
    }
    return dictIds;
  }

  @Override
  public Float getMinVal() {
    return _min;
  }

  @Override
  public Float getMaxVal() {
    return _max;
  }

  @Override
  public float[] getSortedValues() {
    int numValues = length();
    float[] sortedValues = new float[numValues];

    for (int dictId = 0; dictId < numValues; dictId++) {
      sortedValues[dictId] = getFloatValue(dictId);
    }

    Arrays.sort(sortedValues);
    return sortedValues;
  }

  @Override
  public DataType getValueType() {
    return DataType.FLOAT;
  }

  @Override
  public int indexOf(String stringValue) {
    return getDictId(Float.valueOf(stringValue), null);
  }

  @Override
  public int indexOf(float floatValue) {
    return getDictId(floatValue, null);
  }

  public Float get(int dictId) {
    return getFloatValue(dictId);
  }

  @Override
  public int getIntValue(int dictId) {
    return (int) getFloatValue(dictId);
  }

  @Override
  public long getLongValue(int dictId) {
    return (long) getFloatValue(dictId);
  }

  @Override
  public float getFloatValue(int dictId) {
    return _dictIdToValue.getFloat(dictId);
  }

  @Override
  public double getDoubleValue(int dictId) {
    return Double.parseDouble(Float.valueOf(getFloatValue(dictId)).toString());
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    return BigDecimal.valueOf(getFloatValue(dictId));
  }

  @Override
  public String getStringValue(int dictId) {
    return Float.toString(getFloatValue(dictId));
  }

  @Override
  protected void setValue(int dictId, Object value, byte[] serializedValue) {
    _dictIdToValue.setFloat(dictId, (Float) value);
  }

  @Override
  protected boolean equalsValueAt(int dictId, Object value, byte[] serializedValue) {
    return getFloatValue(dictId) == (Float) value;
  }

  @Override
  public int getAvgValueSize() {
    return Float.BYTES;
  }

  @Override
  public long getTotalOffHeapMemUsed() {
    return getOffHeapMemUsed() + Float.BYTES * (long) length();
  }

  @Override
  public void doClose()
      throws IOException {
    _dictIdToValue.close();
  }

  private void updateMinMax(float value) {
    if (value < _min) {
      _min = value;
    }
    if (value > _max) {
      _max = value;
    }
  }
}
