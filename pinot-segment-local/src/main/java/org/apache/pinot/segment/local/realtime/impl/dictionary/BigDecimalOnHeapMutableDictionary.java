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
import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.spi.data.FieldSpec.DataType;


@SuppressWarnings("Duplicates")
public class BigDecimalOnHeapMutableDictionary extends BaseOnHeapMutableDictionary {
  private volatile BigDecimal _min = null;
  private volatile BigDecimal _max = null;

  @Override
  public int index(Object value) {
    BigDecimal bigDecimalValue = (BigDecimal) value;
    updateMinMax(bigDecimalValue);
    return indexValue(bigDecimalValue);
  }

  @Override
  public int[] index(Object[] values) {
    int numValues = values.length;
    int[] dictIds = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      BigDecimal bigDecimalValue = (BigDecimal) values[i];
      updateMinMax(bigDecimalValue);
      dictIds[i] = indexValue(bigDecimalValue);
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
    return getDictId(new BigDecimal(stringValue));
  }

  @Override
  public int indexOf(BigDecimal bigDecimalValue) {
    return getDictId(bigDecimalValue);
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
    return (BigDecimal) get(dictId);
  }

  @Override
  public String getStringValue(int dictId) {
    return getBigDecimalValue(dictId).toPlainString();
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return getBytesValue(dictId);
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
