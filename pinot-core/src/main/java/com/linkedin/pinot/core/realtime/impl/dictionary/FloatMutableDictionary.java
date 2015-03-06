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

public class FloatMutableDictionary extends MutableDictionaryReader {

  public FloatMutableDictionary(FieldSpec spec) {
    super(spec);
  }

  @Override
  public void index(Object rawValue) {
    if (rawValue == null)
      return;

    if (rawValue instanceof String) {
      addToDictionaryBiMap(new Float(Float.parseFloat(rawValue.toString())));
      return;
    }

    if (rawValue instanceof Float) {
      addToDictionaryBiMap(rawValue);
      return;
    }

    if (rawValue instanceof Object[]) {

      for (Object o : (Object[]) rawValue) {
        if (o instanceof String) {
          addToDictionaryBiMap(new Float(Float.parseFloat(o.toString())));
          continue;
        }

        if (o instanceof Float) {
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
      return getIndexOfFromBiMap(new Float(Float.parseFloat(rawValue.toString())));
    }

    return getIndexOfFromBiMap(rawValue);
  }

  @Override
  public Object get(int dictionaryId) {
    return getRawValueFromBiMap(dictionaryId);
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return ((Float)getRawValueFromBiMap(dictionaryId)).longValue();
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return new Double(((Float)getRawValueFromBiMap(dictionaryId)).floatValue());
  }

  @Override
  public String toString(int dictionaryId) {
    return ((Float)getRawValueFromBiMap(dictionaryId)).toString();
  }

  @Override
  public boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper) {

    float lowerInFloat = Float.parseFloat(lower);
    float upperInFloat = Float.parseFloat(upper);

    float valueToCompare = getFloat(indexOfValueToCompare);

    boolean ret = true;

    if (includeLower) {
      if (valueToCompare < lowerInFloat )
        ret = false;
    } else {
      if (valueToCompare <= lowerInFloat)
        ret = false;
    }

    if (includeUpper) {
      if (valueToCompare > upperInFloat )
        ret = false;
    } else {
      if (valueToCompare >= upperInFloat)
        ret = false;
    }

    return ret;
  }

}
