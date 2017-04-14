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


public class DoubleBiMapDictionary extends MutableDictionaryReader {

  private Double min = Double.MAX_VALUE;
  private Double max = Double.MIN_VALUE;

  public DoubleBiMapDictionary(String column) {
    super(column);
  }

  @Override
  public void index(Object rawValue) {
    if (rawValue == null) {
      hasNull = true;
      return;
    }

    if (rawValue instanceof String) {
      Double entry = Double.parseDouble((String) rawValue);
      addToDictionaryBiMap(entry);
      updateMinMax(entry);
      return;
    }

    if (rawValue instanceof Double) {
      addToDictionaryBiMap(rawValue);
      updateMinMax((Double) rawValue);
      return;
    }

    if (rawValue instanceof Object[]) {
      for (Object o : (Object[]) rawValue) {
        if (o instanceof String) {
          final Double doubleValue = Double.parseDouble(o.toString());
          addToDictionaryBiMap(doubleValue);
          updateMinMax(doubleValue);
          continue;
        }

        if (o instanceof Double) {
          addToDictionaryBiMap(o);
          updateMinMax((Double) o);
        }
      }
    }
  }

  private void updateMinMax(Double entry) {
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
      return dictionaryIdBiMap.inverse().containsKey(Double.parseDouble(rawValue.toString()));
    }
    return dictionaryIdBiMap.inverse().containsKey(rawValue);
  }

  @Override
  public int indexOf(Object rawValue) {
    if (rawValue instanceof String) {
      return getIndexOfFromBiMap(Double.parseDouble(rawValue.toString()));
    }
    return getIndexOfFromBiMap(rawValue);
  }

  @Override
  public Object get(int dictionaryId) {
    return getRawValueFromBiMap(dictionaryId);
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return ((Double) getRawValueFromBiMap(dictionaryId)).longValue();
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return (Double) getRawValueFromBiMap(dictionaryId);
  }

  @Override
  public int getIntValue(int dictionaryId) {
    return (int) getDouble(dictionaryId);
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    return (float) getDouble(dictionaryId);
  }

  @Override
  public boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper) {

    double lowerInDouble = Double.parseDouble(lower);
    double upperInDouble = Double.parseDouble(upper);

    double valueToCompare = getDouble(indexOfValueToCompare);

    boolean ret = true;

    if (includeLower) {
      if (valueToCompare < lowerInDouble) {
        ret = false;
      }
    } else {
      if (valueToCompare <= lowerInDouble) {
        ret = false;
      }
    }

    if (includeUpper) {
      if (valueToCompare > upperInDouble) {
        ret = false;
      }
    } else {
      if (valueToCompare >= upperInDouble) {
        ret = false;
      }
    }

    return ret;
  }

  @Override
  public Object getSortedValues() {
    int valueCount = length();
    double[] values = new double[valueCount];

    for (int i = 0; i < valueCount; i++) {
      values[i] = getDoubleValue(i);
    }

    Arrays.sort(values);

    return values;
  }

  @Override
  public String getStringValue(int dictionaryId) {
    return ((Double) getRawValueFromBiMap(dictionaryId)).toString();
  }

  private double getDouble(int dictionaryId) {
    return (Double) dictionaryIdBiMap.get(dictionaryId);
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
