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


public class StringMutableDictionary extends MutableDictionaryReader {

  public StringMutableDictionary(FieldSpec spec) {
    super(spec);
  }

  @Override
  public void index(Object rawValue) {
    if (rawValue == null) {
      hasNull = true;
      return;
    }

    if (rawValue instanceof Object[]) {
      for (Object o : (Object[]) rawValue) {
        if (o != null) {
          addToDictionaryBiMap(o.toString());
        }
      }
      return;
    }

    addToDictionaryBiMap(rawValue.toString());
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
    if (rawValue == null) {
      return 0;
    }
    return getIndexOfFromBiMap(rawValue.toString());
  }

  @Override
  public Object get(int dictionaryId) {
    if (dictionaryId == 0 || dictionaryId > length()) {
      return "null";
    }
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
      if (lower.compareTo(stringToCompare) < 0)
        ret = false;
    } else {
      if (lower.compareTo(stringToCompare) <= 0)
        ret = false;
    }

    if (includeUpper) {
      if (upper.compareTo(stringToCompare) > 0)
        ret = false;
    } else {
      if (upper.compareTo(stringToCompare) >= 0)
        ret = false;
    }

    return ret;
  }

  private String getString(int dictionaryId) {
    return dictionaryIdBiMap.get(new Integer(dictionaryId)).toString();
  }

}
