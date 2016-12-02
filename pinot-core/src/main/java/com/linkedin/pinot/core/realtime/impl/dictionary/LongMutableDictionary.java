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

import com.linkedin.pinot.common.data.FieldSpec;


public class LongMutableDictionary extends MutableDictionaryReader {

  private Long min = Long.MAX_VALUE;
  private Long max = Long.MIN_VALUE;

  public LongMutableDictionary(FieldSpec spec) {
    super(spec);
  }

  @Override
  public void index(Object rawValue) {
    if (rawValue == null) {
      hasNull = true;
      return;
    }

    if (rawValue instanceof String) {
      Long e = Long.parseLong(rawValue.toString());
      addToDictionaryBiMap(e);
      updateMinMax(e);
      return;
    }

    if (rawValue instanceof Long) {
      addToDictionaryBiMap(rawValue);
      updateMinMax((Long) rawValue);
      return;
    }

    if (rawValue instanceof Object[]) {
      for (Object o : (Object[]) rawValue) {
        if (o instanceof String) {
          final Long longValue = Long.parseLong(o.toString());
          addToDictionaryBiMap(longValue);
          updateMinMax(longValue);
          continue;
        }

        if (o instanceof Long) {
          addToDictionaryBiMap(o);
          updateMinMax((Long) o);
        }
      }
    }
  }

  private void updateMinMax(Long entry) {
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
      return dictionaryIdBiMap.inverse().containsKey(Long.parseLong((String) rawValue));
    }
    return dictionaryIdBiMap.inverse().containsKey(rawValue);
  }

  @Override
  public int indexOf(Object rawValue) {
    if (rawValue instanceof String) {
      return getIndexOfFromBiMap(Long.parseLong(rawValue.toString()));
    }
    return getIndexOfFromBiMap(rawValue);
  }

  @Override
  public Object get(int dictionaryId) {
    return getRawValueFromBiMap(dictionaryId);
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return (Long) getRawValueFromBiMap(dictionaryId);
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return ((Long) getRawValueFromBiMap(dictionaryId)).doubleValue();
  }

  @Override
  public int getIntValue(int dictionaryId) {
    return ((Long) getRawValueFromBiMap(dictionaryId)).intValue();
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    return ((Long) getRawValueFromBiMap(dictionaryId)).floatValue();
  }

  @Override
  public String toString(int dictionaryId) {
    return ((Long) getRawValueFromBiMap(dictionaryId)).toString();
  }


  @Override
  public boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper) {

    long lowerInLong = Long.parseLong(lower);
    long upperInLong = Long.parseLong(upper);

    long valueToCompare = getLong(indexOfValueToCompare);

    boolean ret = true;

    if (includeLower) {
      if (valueToCompare < lowerInLong) {
        ret = false;
      }
    } else {
      if (valueToCompare <= lowerInLong) {
        ret = false;
      }
    }

    if (includeUpper) {
      if (valueToCompare > upperInLong) {
        ret = false;
      }
    } else {
      if (valueToCompare >= upperInLong) {
        ret = false;
      }
    }

    return ret;
  }

  @Override
  public String getStringValue(int dictionaryId) {
    return getRawValueFromBiMap(dictionaryId).toString();
  }

  private long getLong(int dictionaryId) {
    return (Long) dictionaryIdBiMap.get(dictionaryId);
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
