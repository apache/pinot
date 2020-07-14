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
package org.apache.pinot.core.realtime.impl.dictionary;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import java.util.Arrays;
import org.apache.pinot.core.query.request.context.predicate.RangePredicate;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BytesUtils;


@SuppressWarnings("Duplicates")
public class StringOnHeapMutableDictionary extends BaseOnHeapMutableDictionary {
  private volatile String _min = null;
  private volatile String _max = null;

  @Override
  public int index(Object value) {
    String stringValue = (String) value;
    updateMinMax(stringValue);
    return indexValue(stringValue);
  }

  @Override
  public int[] index(Object[] values) {
    int numValues = values.length;
    int[] dictIds = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      String stringValue = (String) values[i];
      updateMinMax(stringValue);
      dictIds[i] = indexValue(stringValue);
    }
    return dictIds;
  }

  @Override
  public int compare(int dictId1, int dictId2) {
    return getStringValue(dictId1).compareTo(getStringValue(dictId2));
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
      for (int dictId = 0; dictId < numValues; dictId++) {
        String value = getStringValue(dictId);
        if (value.compareTo(upper) <= upperCompareThreshold) {
          dictIds.add(dictId);
        }
      }
    } else if (upper.equals(RangePredicate.UNBOUNDED)) {
      for (int dictId = 0; dictId < numValues; dictId++) {
        String value = getStringValue(dictId);
        if (value.compareTo(lower) >= lowerCompareThreshold) {
          dictIds.add(dictId);
        }
      }
    } else {
      for (int dictId = 0; dictId < numValues; dictId++) {
        String value = getStringValue(dictId);
        if (value.compareTo(lower) >= lowerCompareThreshold && value.compareTo(upper) <= upperCompareThreshold) {
          dictIds.add(dictId);
        }
      }
    }
    return dictIds;
  }

  @Override
  public String getMinVal() {
    return _min;
  }

  @Override
  public String getMaxVal() {
    return _max;
  }

  @Override
  public String[] getSortedValues() {
    int numValues = length();
    String[] sortedValues = new String[numValues];

    for (int dictId = 0; dictId < numValues; dictId++) {
      sortedValues[dictId] = getStringValue(dictId);
    }

    Arrays.sort(sortedValues);
    return sortedValues;
  }

  @Override
  public DataType getValueType() {
    return DataType.STRING;
  }

  @Override
  public int indexOf(String stringValue) {
    return getDictId(stringValue);
  }

  @Override
  public int getIntValue(int dictId) {
    return Integer.parseInt(getStringValue(dictId));
  }

  @Override
  public long getLongValue(int dictId) {
    return Long.parseLong(getStringValue(dictId));
  }

  @Override
  public float getFloatValue(int dictId) {
    return Float.parseFloat(getStringValue(dictId));
  }

  @Override
  public double getDoubleValue(int dictId) {
    return Double.parseDouble(getStringValue(dictId));
  }

  @Override
  public String getStringValue(int dictId) {
    return (String) get(dictId);
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return BytesUtils.toBytes(getStringValue(dictId));
  }

  private void updateMinMax(String value) {
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
