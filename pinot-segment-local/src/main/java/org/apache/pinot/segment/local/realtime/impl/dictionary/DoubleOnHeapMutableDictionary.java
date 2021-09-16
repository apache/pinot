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
import java.util.Arrays;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.request.context.predicate.RangePredicate;


@SuppressWarnings("Duplicates")
public class DoubleOnHeapMutableDictionary extends BaseOnHeapMutableDictionary {
  private volatile double _min = Double.MAX_VALUE;
  private volatile double _max = Double.MIN_VALUE;

  @Override
  public int index(Object value) {
    Double doubleValue = (Double) value;
    updateMinMax(doubleValue);
    return indexValue(doubleValue);
  }

  @Override
  public int[] index(Object[] values) {
    int numValues = values.length;
    int[] dictIds = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      Double doubleValue = (Double) values[i];
      updateMinMax(doubleValue);
      dictIds[i] = indexValue(doubleValue);
    }
    return dictIds;
  }

  @Override
  public int compare(int dictId1, int dictId2) {
    return Double.compare(getDoubleValue(dictId1), getDoubleValue(dictId2));
  }

  @Override
  public IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper) {
    int numValues = length();
    if (numValues == 0) {
      return IntSets.EMPTY_SET;
    }
    IntSet dictIds = new IntOpenHashSet();

    if (lower.equals(RangePredicate.UNBOUNDED)) {
      double upperValue = Double.parseDouble(upper);
      if (includeUpper) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          double value = getDoubleValue(dictId);
          if (value <= upperValue) {
            dictIds.add(dictId);
          }
        }
      } else {
        for (int dictId = 0; dictId < numValues; dictId++) {
          double value = getDoubleValue(dictId);
          if (value < upperValue) {
            dictIds.add(dictId);
          }
        }
      }
    } else if (upper.equals(RangePredicate.UNBOUNDED)) {
      double lowerValue = Double.parseDouble(lower);
      if (includeLower) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          double value = getDoubleValue(dictId);
          if (value >= lowerValue) {
            dictIds.add(dictId);
          }
        }
      } else {
        for (int dictId = 0; dictId < numValues; dictId++) {
          double value = getDoubleValue(dictId);
          if (value > lowerValue) {
            dictIds.add(dictId);
          }
        }
      }
    } else {
      double lowerValue = Double.parseDouble(lower);
      double upperValue = Double.parseDouble(upper);
      if (includeLower && includeUpper) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          double value = getDoubleValue(dictId);
          if (value >= lowerValue && value <= upperValue) {
            dictIds.add(dictId);
          }
        }
      } else if (includeLower) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          double value = getDoubleValue(dictId);
          if (value >= lowerValue && value < upperValue) {
            dictIds.add(dictId);
          }
        }
      } else if (includeUpper) {
        for (int dictId = 0; dictId < numValues; dictId++) {
          double value = getDoubleValue(dictId);
          if (value > lowerValue && value <= upperValue) {
            dictIds.add(dictId);
          }
        }
      } else {
        for (int dictId = 0; dictId < numValues; dictId++) {
          double value = getDoubleValue(dictId);
          if (value > lowerValue && value < upperValue) {
            dictIds.add(dictId);
          }
        }
      }
    }
    return dictIds;
  }

  @Override
  public Double getMinVal() {
    return _min;
  }

  @Override
  public Double getMaxVal() {
    return _max;
  }

  @Override
  public double[] getSortedValues() {
    int numValues = length();
    double[] sortedValues = new double[numValues];

    for (int dictId = 0; dictId < numValues; dictId++) {
      sortedValues[dictId] = getDoubleValue(dictId);
    }

    Arrays.sort(sortedValues);
    return sortedValues;
  }

  @Override
  public DataType getValueType() {
    return DataType.DOUBLE;
  }

  @Override
  public int indexOf(String stringValue) {
    return getDictId(Double.valueOf(stringValue));
  }

  @Override
  public int getIntValue(int dictId) {
    return (int) getDoubleValue(dictId);
  }

  @Override
  public long getLongValue(int dictId) {
    return (long) getDoubleValue(dictId);
  }

  @Override
  public float getFloatValue(int dictId) {
    return (float) getDoubleValue(dictId);
  }

  @Override
  public double getDoubleValue(int dictId) {
    return (Double) get(dictId);
  }

  @Override
  public String getStringValue(int dictId) {
    return Double.toString(getDoubleValue(dictId));
  }

  private void updateMinMax(double value) {
    if (value < _min) {
      _min = value;
    }
    if (value > _max) {
      _max = value;
    }
  }
}
