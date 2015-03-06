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


public class DoubleMutableDictionary extends MutableDictionaryReader {

  public DoubleMutableDictionary(FieldSpec spec) {
    super(spec);
  }

  @Override
  public void index(Object rawValue) {
    if (rawValue == null)
      return;

    if (rawValue instanceof String) {
      addToDictionaryBiMap(new Double(Double.parseDouble(rawValue.toString())));
      return;
    }

    if (rawValue instanceof Double) {
      addToDictionaryBiMap(rawValue);
      return;
    }

    if (rawValue instanceof Object[]) {
      for (Object o : (Object[]) rawValue) {
        if (o instanceof String) {
          addToDictionaryBiMap(new Double(Double.parseDouble(o.toString())));
          continue;
        }

        if (o instanceof Double) {
          addToDictionaryBiMap(o);
          continue;
        }
      }
    }

  }

  @Override
  public int indexOf(Object rawValue) {
    if (rawValue == null)
      return 0;

    if (rawValue instanceof String) {
      return getIndexOfFromBiMap(new Double(Double.parseDouble(rawValue.toString())));
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
    return ((Double) getRawValueFromBiMap(dictionaryId)).doubleValue();
  }

  @Override
  public String toString(int dictionaryId) {
    return ((Double) getRawValueFromBiMap(dictionaryId)).toString();
  }

  @Override
  public boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper) {

    double lowerInDouble = Double.parseDouble(lower);
    double upperInDouble = Double.parseDouble(upper);

    double valueToCompare = getDouble(indexOfValueToCompare);

    boolean ret = true;

    if (includeLower) {
      if (valueToCompare < lowerInDouble )
        ret = false;
    } else {
      if (valueToCompare <= lowerInDouble)
        ret = false;
    }

    if (includeUpper) {
      if (valueToCompare > upperInDouble )
        ret = false;
    } else {
      if (valueToCompare >= upperInDouble)
        ret = false;
    }

    return ret;
  }

}
