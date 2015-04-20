/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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


public class IntMutableDictionary extends MutableDictionaryReader {

  public IntMutableDictionary(FieldSpec spec) {
    super(spec);
  }

  @Override
  public void index(Object rawValue) {
    if (rawValue == null) {
      hasNull = true;
      return;
    }

    if (rawValue instanceof String) {
      addToDictionaryBiMap(new Integer(Integer.parseInt(rawValue.toString())));
      return;
    }

    if (rawValue instanceof Integer) {
      addToDictionaryBiMap(rawValue);
      return;
    }

    if (rawValue instanceof Object[]) {
      Object[] multivalues = (Object[]) rawValue;

      for (int i = 0; i < multivalues.length; i++) {

        if (multivalues[i] instanceof String) {
          addToDictionaryBiMap(new Integer(Integer.parseInt(multivalues[i].toString())));
          continue;
        }

        if (multivalues[i] instanceof Integer) {
          addToDictionaryBiMap(multivalues[i]);
          continue;
        }
      }
    }
  }

  @Override
  public boolean contains(Object rawValue) {
    if (rawValue == null) {
      return hasNull;
    }
    if (rawValue instanceof String) {
      return dictionaryIdBiMap.inverse().containsKey(new Integer(Integer.parseInt(rawValue.toString())));
    }
    return dictionaryIdBiMap.inverse().containsKey(rawValue);
  }

  @Override
  public int indexOf(Object rawValue) {
    if (rawValue == null) {
      return 0;
    }
    if (rawValue instanceof String) {
      return getIndexOfFromBiMap(new Integer(Integer.parseInt(rawValue.toString())));
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
  public String toString(int dictionaryId) {
    return ((Integer) getRawValueFromBiMap(dictionaryId)).toString();
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
    return ((Integer) dictionaryIdBiMap.get(new Integer(dictionaryId))).intValue();
  }

}
