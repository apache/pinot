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


public class FloatMutableDictionary extends MutableDictionaryReader {

  private Float min = Float.MAX_VALUE;
  private Float max = Float.MIN_VALUE;

  public FloatMutableDictionary(String column) {
    super(column);
  }

  @Override
  public void index(Object rawValue) {
    if (rawValue == null) {
      hasNull = true;
      return;
    }
    if (rawValue instanceof String) {
      Float e = Float.parseFloat(rawValue.toString());
      addToDictionaryBiMap(e);
      updateMinMax(e);
      return;
    }

    if (rawValue instanceof Float) {
      addToDictionaryBiMap(rawValue);
      updateMinMax((Float) rawValue);
      return;
    }

    if (rawValue instanceof Object[]) {

      for (Object o : (Object[]) rawValue) {
        if (o instanceof String) {
          final Float floatValue = Float.parseFloat(o.toString());
          addToDictionaryBiMap(floatValue);
          updateMinMax(floatValue);
          continue;
        }

        if (o instanceof Float) {
          addToDictionaryBiMap(o);
          updateMinMax((Float) o);
        }
      }
    }
  }

  private void updateMinMax(Float entry) {
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
      return dictionaryIdBiMap.inverse().containsKey(Float.parseFloat(rawValue.toString()));
    }
    return dictionaryIdBiMap.inverse().containsKey(rawValue);
  }

  @Override
  public int indexOf(Object rawValue) {
    if (rawValue instanceof String) {
      return getIndexOfFromBiMap(Float.parseFloat(rawValue.toString()));
    }
    return getIndexOfFromBiMap(rawValue);
  }

  @Override
  public Object get(int dictionaryId) {
    return getRawValueFromBiMap(dictionaryId);
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return ((Float) getRawValueFromBiMap(dictionaryId)).longValue();
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return ((Float) getRawValueFromBiMap(dictionaryId)).doubleValue();
  }

  @Override
  public int getIntValue(int dictionaryId) {
    return ((Float) getRawValueFromBiMap(dictionaryId)).intValue();
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    return ((Float) getRawValueFromBiMap(dictionaryId));
  }

  @Override
  public String toString(int dictionaryId) {
    return (getRawValueFromBiMap(dictionaryId)).toString();
  }

  @Override
  public boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper) {

    float lowerInFloat = Float.parseFloat(lower);
    float upperInFloat = Float.parseFloat(upper);

    float valueToCompare = getFloat(indexOfValueToCompare);

    boolean ret = true;

    if (includeLower) {
      if (valueToCompare < lowerInFloat) {
        ret = false;
      }
    } else {
      if (valueToCompare <= lowerInFloat) {
        ret = false;
      }
    }

    if (includeUpper) {
      if (valueToCompare > upperInFloat) {
        ret = false;
      }
    } else {
      if (valueToCompare >= upperInFloat) {
        ret = false;
      }
    }

    return ret;
  }

  @Override
  public Object getSortedValues() {
    int valueCount = length();
    float[] values = new float[valueCount];

    for (int i = 0; i < valueCount; i++) {
      values[i] = getFloatValue(i);
    }

    Arrays.sort(values);

    return values;
  }

  @Override
  public String getStringValue(int dictionaryId) {
    return (getRawValueFromBiMap(dictionaryId)).toString();
  }

  private float getFloat(int dictionaryId) {
    return (Float) dictionaryIdBiMap.get(dictionaryId);
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
