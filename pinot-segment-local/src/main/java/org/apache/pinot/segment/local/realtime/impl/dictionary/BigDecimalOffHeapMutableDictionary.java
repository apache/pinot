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
import org.apache.pinot.segment.local.io.writer.impl.MutableOffHeapByteArrayStore;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;


@SuppressWarnings("Duplicates")
public class BigDecimalOffHeapMutableDictionary extends BaseOffHeapMutableDictionary {
  private final MutableOffHeapByteArrayStore _byteStore;

  private volatile BigDecimal _min = null;
  private volatile BigDecimal _max = null;

  public BigDecimalOffHeapMutableDictionary(int estimatedCardinality, int maxOverflowHashSize,
      PinotDataBufferMemoryManager memoryManager, String allocationContext, int avgBigDecimalLen) {
    super(estimatedCardinality, maxOverflowHashSize, memoryManager, allocationContext);
    _byteStore =
        new MutableOffHeapByteArrayStore(memoryManager, allocationContext, estimatedCardinality, avgBigDecimalLen);
  }

  @Override
  public int index(Object value) {
    BigDecimal bigDecimalValue = (BigDecimal) value;
    updateMinMax(bigDecimalValue);
    return indexValue(bigDecimalValue, BigDecimalUtils.serialize(bigDecimalValue));
  }

  @Override
  public int[] index(Object[] values) {
    int numValues = values.length;
    int[] dictIds = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      BigDecimal bigDecimalValue = (BigDecimal) values[i];
      updateMinMax(bigDecimalValue);
      dictIds[i] = indexValue(bigDecimalValue, BigDecimalUtils.serialize(bigDecimalValue));
    }
    return dictIds;
  }

  @Override
  public int compare(int dictId1, int dictId2) {
    return getBigDecimalValue(dictId1).compareTo(getBigDecimalValue(dictId2));
  }

  @Override
  public IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper) {
    int numValues = length();
    if (numValues == 0) {
      return IntSets.EMPTY_SET;
    }
    IntSet dictIds = new IntOpenHashSet();

    if (lower.equals(RangePredicate.UNBOUNDED)) {
      BigDecimal upperValue = new BigDecimal(upper);
      if (includeUpper) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          BigDecimal value = getBigDecimalValue(dictId);
          if (value.compareTo(upperValue) <= 0) {
            dictIds.add(dictId);
          }
        }
      } else {
        for (int dictId = 0; dictId < numValues; dictId++) {
          BigDecimal value = getBigDecimalValue(dictId);
          if (value.compareTo(upperValue) < 0) {
            dictIds.add(dictId);
          }
        }
      }
    } else if (upper.equals(RangePredicate.UNBOUNDED)) {
      BigDecimal lowerValue = new BigDecimal(lower);
      if (includeLower) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          BigDecimal value = getBigDecimalValue(dictId);
          if (value.compareTo(lowerValue) >= 0) {
            dictIds.add(dictId);
          }
        }
      } else {
        for (int dictId = 0; dictId < numValues; dictId++) {
          BigDecimal value = getBigDecimalValue(dictId);
          if (value.compareTo(lowerValue) > 0) {
            dictIds.add(dictId);
          }
        }
      }
    } else {
      BigDecimal lowerValue = new BigDecimal(lower);
      BigDecimal upperValue = new BigDecimal(upper);
      if (includeLower && includeUpper) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          BigDecimal value = getBigDecimalValue(dictId);
          if (value.compareTo(lowerValue) >= 0 && value.compareTo(upperValue) <= 0) {
            dictIds.add(dictId);
          }
        }
      } else if (includeLower) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          BigDecimal value = getBigDecimalValue(dictId);
          if (value.compareTo(lowerValue) >= 0 && value.compareTo(upperValue) < 0) {
            dictIds.add(dictId);
          }
        }
      } else if (includeUpper) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          BigDecimal value = getBigDecimalValue(dictId);
          if (value.compareTo(lowerValue) > 0 && value.compareTo(upperValue) <= 0) {
            dictIds.add(dictId);
          }
        }
      } else {
        for (int dictId = 0; dictId < numValues; dictId++) {
          BigDecimal value = getBigDecimalValue(dictId);
          if (value.compareTo(lowerValue) > 0 && value.compareTo(upperValue) < 0) {
            dictIds.add(dictId);
          }
        }
      }
    }
    return dictIds;
  }

  @Override
  public BigDecimal getMinVal() {
    return _min;
  }

  @Override
  public BigDecimal getMaxVal() {
    return _max;
  }

  @Override
  public BigDecimal[] getSortedValues() {
    int numValues = length();
    BigDecimal[] sortedValues = new BigDecimal[numValues];

    for (int dictId = 0; dictId < numValues; dictId++) {
      sortedValues[dictId] = getBigDecimalValue(dictId);
    }

    Arrays.sort(sortedValues);
    return sortedValues;
  }

  @Override
  public DataType getValueType() {
    return DataType.BIG_DECIMAL;
  }

  @Override
  public int indexOf(String stringValue) {
    BigDecimal bigDecimalValue = new BigDecimal(stringValue);
    return getDictId(bigDecimalValue, BigDecimalUtils.serialize(bigDecimalValue));
  }

  @Override
  public int indexOf(BigDecimal bigDecimalValue) {
    return getDictId(bigDecimalValue, BigDecimalUtils.serialize(bigDecimalValue));
  }

  public BigDecimal get(int dictId) {
    return getBigDecimalValue(dictId);
  }

  @Override
  public int getIntValue(int dictId) {
    return getBigDecimalValue(dictId).intValue();
  }

  @Override
  public long getLongValue(int dictId) {
    return getBigDecimalValue(dictId).longValue();
  }

  @Override
  public float getFloatValue(int dictId) {
    return getBigDecimalValue(dictId).floatValue();
  }

  @Override
  public double getDoubleValue(int dictId) {
    return getBigDecimalValue(dictId).doubleValue();
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    return BigDecimalUtils.deserialize(_byteStore.get(dictId));
  }

  @Override
  public String getStringValue(int dictId) {
    return getBigDecimalValue(dictId).toPlainString();
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

  private void updateMinMax(BigDecimal value) {
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
}
