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

import java.util.Arrays;
import javax.annotation.Nonnull;


public class DoubleOnHeapMutableDictionary extends BaseOnHeapMutableDictionary {
  private double _min = Double.MAX_VALUE;
  private double _max = Double.MIN_VALUE;

  @Override
  public int indexOf(Object rawValue) {
    if (rawValue instanceof String) {
      return getDictId(Double.valueOf((String) rawValue));
    } else {
      return getDictId(rawValue);
    }
  }

  @Override
  public void index(@Nonnull Object rawValue) {
    if (rawValue instanceof Double) {
      // Single value
      indexValue(rawValue);
      updateMinMax((Double) rawValue);
    } else {
      // Multi value
      Object[] values = (Object[]) rawValue;
      for (Object value : values) {
        indexValue(value);
        updateMinMax((Double) value);
      }
    }
  }

  @SuppressWarnings("Duplicates")
  @Override
  public boolean inRange(@Nonnull String lower, @Nonnull String upper, int dictIdToCompare, boolean includeLower,
      boolean includeUpper) {
    double lowerDouble = Double.parseDouble(lower);
    double upperDouble = Double.parseDouble(upper);
    double valueToCompare = (Double) get(dictIdToCompare);

    if (includeLower) {
      if (valueToCompare < lowerDouble) {
        return false;
      }
    } else {
      if (valueToCompare <= lowerDouble) {
        return false;
      }
    }

    if (includeUpper) {
      if (valueToCompare > upperDouble) {
        return false;
      }
    } else {
      if (valueToCompare >= upperDouble) {
        return false;
      }
    }

    return true;
  }

  @Nonnull
  @Override
  public Double getMinVal() {
    return _min;
  }

  @Nonnull
  @Override
  public Double getMaxVal() {
    return _max;
  }

  @Nonnull
  @Override
  public double[] getSortedValues() {
    int numValues = length();
    double[] sortedValues = new double[numValues];

    for (int i = 0; i < numValues; i++) {
      sortedValues[i] = (Double) get(i);
    }

    Arrays.sort(sortedValues);
    return sortedValues;
  }

  @Override
  public int getIntValue(int dictId) {
    return ((Double) get(dictId)).intValue();
  }

  @Override
  public long getLongValue(int dictId) {
    return ((Double) get(dictId)).longValue();
  }

  @Override
  public float getFloatValue(int dictId) {
    return ((Double) get(dictId)).floatValue();
  }

  @Override
  public double getDoubleValue(int dictId) {
    return (Double) get(dictId);
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
