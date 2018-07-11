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

import java.util.Arrays;
import javax.annotation.Nonnull;


public class StringOnHeapMutableDictionary extends BaseOnHeapMutableDictionary {
  private String _min = null;
  private String _max = null;

  @Override
  public int indexOf(Object rawValue) {
    return getDictId(rawValue);
  }

  @Override
  public void index(@Nonnull Object rawValue) {
    if (rawValue instanceof String) {
      // Single value
      indexValue(rawValue);
      updateMinMax((String) rawValue);
    } else {
      // Multi value
      Object[] values = (Object[]) rawValue;
      for (Object value : values) {
        indexValue(value);
        updateMinMax((String) value);
      }
    }
  }

  @Override
  public boolean inRange(@Nonnull String lower, @Nonnull String upper, int dictIdToCompare, boolean includeLower,
      boolean includeUpper) {
    String valueToCompare = (String) get(dictIdToCompare);
    return valueInRange(lower, upper, includeLower, includeUpper, valueToCompare);
  }

  @Nonnull
  @Override
  public String getMinVal() {
    return _min;
  }

  @Nonnull
  @Override
  public String getMaxVal() {
    return _max;
  }

  @Nonnull
  @Override
  public String[] getSortedValues() {
    int numValues = length();
    String[] sortedValues = new String[numValues];

    for (int i = 0; i < numValues; i++) {
      sortedValues[i] = (String) get(i);
    }

    Arrays.sort(sortedValues);
    return sortedValues;
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
