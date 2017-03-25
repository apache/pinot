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


public class StringMutableDictionary extends MutableDictionaryReader {

  private String min = null;
  private String max = null;

  public StringMutableDictionary(String column) {
    super(column);
  }

  @Override
  public void index(Object rawValue) {
    if (rawValue instanceof Object[]) {
      for (Object o : (Object[]) rawValue) {
        addToDictionaryBiMap(o.toString());
        updateMinMax(o.toString());
      }
      return;
    }

    addToDictionaryBiMap(rawValue.toString());
    updateMinMax(rawValue.toString());
  }

  private void updateMinMax(String entry) {
    if (min == null && max == null) {
      min = entry;
      max = entry;
      return;
    }

    if (entry.compareTo(min) < 0) {
      min = entry;
    }

    if (entry.compareTo(max) > 0) {
      max = entry;
    }
  }

  @Override
  public boolean contains(Object rawValue) {
    if (rawValue == null) {
      return hasNull;
    }
    return dictionaryIdBiMap.inverse().containsKey(rawValue.toString());
  }

  @Override
  public int indexOf(Object rawValue) {
    return getIndexOfFromBiMap(rawValue.toString());
  }

  @Override
  public Object get(int dictionaryId) {
    return getRawValueFromBiMap(dictionaryId);
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return -1;
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return -1;
  }

  @Override
  public int getIntValue(int dictionaryId) {
    return -1;
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    return -1;
  }

  @Override
  public String toString(int dictionaryId) {
    return (String) getRawValueFromBiMap(dictionaryId);
  }

  @Override
  public String getStringValue(int dictionaryId) {
    return (String) getRawValueFromBiMap(dictionaryId);
  }

  @Override
  public boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper) {

    boolean ret = true;
    String stringToCompare = getString(indexOfValueToCompare);

    if (includeLower) {
      if (lower.compareTo(stringToCompare) > 0) {
        ret = false;
      }
    } else {
      if (lower.compareTo(stringToCompare) >= 0) {
        ret = false;
      }
    }

    if (includeUpper) {
      if (upper.compareTo(stringToCompare) < 0) {
        ret = false;
      }
    } else {
      if (upper.compareTo(stringToCompare) <= 0) {
        ret = false;
      }
    }

    return ret;
  }

  @Override
  public Object getSortedValues() {
    int valueCount = length();
    Object[] values = new Object[valueCount];

    for (int i = 0; i < valueCount; i++) {
      values[i] = getStringValue(i);
    }

    Arrays.sort(values);

    return values;
  }

  private String getString(int dictionaryId) {
    return dictionaryIdBiMap.get(dictionaryId).toString();
  }

  @Override
  public Object getMinVal() {
    return min;
  }

  @Override
  public Object getMaxVal() {
    return max;
  }

}
