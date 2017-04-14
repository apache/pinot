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


public class IntBiMapDictionary extends MutableDictionaryReader {

  private Integer min = Integer.MAX_VALUE;
  private Integer max = Integer.MIN_VALUE;

  public IntBiMapDictionary(String column) {
    super(column);
  }

  @Override
  public void index(Object rawValue) {
    if (rawValue == null) {
      hasNull = true;
      return;
    }

    if (rawValue instanceof String) {
      Integer entry = Integer.parseInt(rawValue.toString());
      addToDictionaryBiMap(entry);
      updateMinMax(entry);
      return;
    }

    if (rawValue instanceof Integer) {
      addToDictionaryBiMap(rawValue);
      updateMinMax((Integer) rawValue);
      return;
    }

    if (rawValue instanceof Object[]) {
      Object[] multivalues = (Object[]) rawValue;

      for (int i = 0; i < multivalues.length; i++) {

        if (multivalues[i] instanceof String) {
          addToDictionaryBiMap(Integer.parseInt(multivalues[i].toString()));
          updateMinMax(Integer.parseInt(multivalues[i].toString()));
          continue;
        }

        if (multivalues[i] instanceof Integer) {
          addToDictionaryBiMap(multivalues[i]);
          updateMinMax((Integer) multivalues[i]);
          continue;
        }
      }
    }
  }

  private void updateMinMax(Integer entry) {
    if (entry < min) {
      min = entry;
    }
    if (entry > max) {
      max = entry;
    }
  }

  @Override
  public boolean contains(Object rawValue) {
    if (rawValue == null) {
      return hasNull;
    }
    if (rawValue instanceof String) {
      return dictionaryIdBiMap.inverse().containsKey(Integer.parseInt(rawValue.toString()));
    }
    return dictionaryIdBiMap.inverse().containsKey(rawValue);
  }

  @Override
  public int indexOf(Object rawValue) {
    if (rawValue instanceof String) {
      return getIndexOfFromBiMap(Integer.parseInt(rawValue.toString()));
    }
    return getIndexOfFromBiMap(rawValue);
  }

  @Override
  public Object get(int dictionaryId) {
    return getRawValueFromBiMap(dictionaryId);
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return ((Integer) getRawValueFromBiMap(dictionaryId)).longValue();
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return ((Integer) getRawValueFromBiMap(dictionaryId)).doubleValue();
  }

  @Override
  public int getIntValue(int dictionaryId) {
    return getInt(dictionaryId);
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    return (float) getInt(dictionaryId);
  }

  @Override
  public void readIntValues(int[] dictionaryIds, int startPos, int limit, int[] outValues, int outStartPos) {
    int endPos = startPos + limit;
    for (int iter = startPos; iter < endPos; ++iter) {
      int dictId = dictionaryIds[iter];
      outValues[outStartPos++] = getInt(dictId);
    }
  }

  @Override
  public Object getSortedValues() {
    int valueCount = length();
    int[] values = new int[valueCount];

    for (int i = 0; i < valueCount; i++) {
      values[i] = getIntValue(i);
    }

    Arrays.sort(values);

    return values;
  }

  @Override
  public String getStringValue(int dictionaryId) {
    return ((Integer) getRawValueFromBiMap(dictionaryId)).toString();
  }

  @Override
  public boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper) {

    int lowerInInt = Integer.parseInt(lower);
    int upperInInt = Integer.parseInt(upper);

    int valueToCompare = getInt(indexOfValueToCompare);

    boolean ret = true;

    if (includeLower) {
      if (valueToCompare < lowerInInt) {
        ret = false;
      }
    } else {
      if (valueToCompare <= lowerInInt) {
        ret = false;
      }
    }

    if (includeUpper) {
      if (valueToCompare > upperInInt) {
        ret = false;
      }
    } else {
      if (valueToCompare >= upperInInt) {
        ret = false;
      }
    }

    return ret;
  }

  public int getInt(int dictionaryId) {
    return (Integer) dictionaryIdBiMap.get(dictionaryId);
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
