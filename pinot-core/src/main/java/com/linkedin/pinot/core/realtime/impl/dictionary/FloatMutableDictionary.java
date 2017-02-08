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

import it.unimi.dsi.fastutil.floats.Float2IntMap;
import it.unimi.dsi.fastutil.floats.Float2IntOpenHashMap;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatSet;


public class FloatMutableDictionary extends MutableDictionaryReader {

  private Float2IntBiMap _dictionaryIdBiMap;
  private String _column;
  private float _min = Float.MAX_VALUE;
  private float _max = Float.MIN_VALUE;

  public FloatMutableDictionary(String column) {
    _column = column;
    _dictionaryIdBiMap = new Float2IntBiMap();
  }

  @Override
  public void index(Object rawValue) {
    if (rawValue == null) {
      hasNull = true;
      return;
    }

    if (rawValue instanceof Object[]) {
      for (Object o : (Object[]) rawValue) {
        indexValue(o);
      }
    } else {
      indexValue(rawValue);
    }
  }

  private void indexValue(Object value) {
    if (value instanceof String) {
      float e = Float.parseFloat((String) value);
      _dictionaryIdBiMap.put(e);
      updateMinMax(e);
    } else if (value instanceof Float) {
      Float floatValue = (Float) value;
      _dictionaryIdBiMap.put(floatValue);
      updateMinMax(floatValue);
    }
  }

  private void updateMinMax(float entry) {
    if (entry < _min) {
      _min = entry;
    } else if (entry > _max) {
      _max = entry;
    }
  }

  @Override
  public boolean contains(Object rawValue) {
    if (rawValue == null) {
      return hasNull;
    }
    if (rawValue instanceof String) {
      return _dictionaryIdBiMap.containsKey(Float.parseFloat(rawValue.toString()));
    }
    return _dictionaryIdBiMap.containsKey((Float) rawValue);
  }

  @Override
  public int indexOf(Object rawValue) {
    if (rawValue instanceof String) {
      return _dictionaryIdBiMap.getValue(Float.parseFloat(rawValue.toString()));
    }
    return _dictionaryIdBiMap.getValue((Float) rawValue);
  }

  @Override
  public Object get(int dictionaryId) {
    return _dictionaryIdBiMap.getKey(dictionaryId);
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return (_dictionaryIdBiMap.getKey(dictionaryId)).longValue();
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return (_dictionaryIdBiMap.getKey(dictionaryId)).doubleValue();
  }

  @Override
  public int getIntValue(int dictionaryId) {
    return (_dictionaryIdBiMap.getKey(dictionaryId)).intValue();
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    return _dictionaryIdBiMap.getKey(dictionaryId);
  }

  @Override
  public String toString(int dictionaryId) {
    return (_dictionaryIdBiMap.getKey(dictionaryId)).toString();
  }

  @Override
  public int length() {
    return _dictionaryIdBiMap.size();
  }

  @Override
  public boolean isEmpty() {
    return _dictionaryIdBiMap.isEmpty();
  }

  @Override
  public boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper) {

    float lowerInFloat = Float.parseFloat(lower);
    float upperInFloat = Float.parseFloat(upper);

    float valueToCompare = _dictionaryIdBiMap.getKey(indexOfValueToCompare);

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
  public String getStringValue(int dictionaryId) {
    return (_dictionaryIdBiMap.getKey(dictionaryId)).toString();
  }

  @Override
  public Object getMinVal() {
    return _min;
  }

  @Override
  public Object getMaxVal() {
    return _max;
  }

  @Override
  public void print() {
    System.out.println("************* printing dictionary for column : " + _column + " ***************");
    for (float key : _dictionaryIdBiMap.keySet()) {
      System.out.println(key + "," + _dictionaryIdBiMap.getValue(key));
    }
    System.out.println("************************************");
  }

  public static class Float2IntBiMap {
    private Float2IntMap _valueToDictIdMap;
    private FloatArrayList _dictIdToValueMap;

    public Float2IntBiMap() {
      _valueToDictIdMap = new Float2IntOpenHashMap();
      _dictIdToValueMap = new FloatArrayList();
      _valueToDictIdMap.defaultReturnValue(INVALID_ID);
    }

    public void put(float value) {
      if (!_valueToDictIdMap.containsKey(value)) {
        _dictIdToValueMap.add(value);
        _valueToDictIdMap.put(value, _dictIdToValueMap.size() - 1);
      }
    }

    public Float getKey(int dictId) {
      return (dictId < _dictIdToValueMap.size()) ? _dictIdToValueMap.getFloat(dictId) : null;
    }

    public int getValue(float value) {
      return _valueToDictIdMap.get(value);
    }

    public boolean containsKey(float value) {
      return _valueToDictIdMap.containsKey(value);
    }

    public int size() {
      return _valueToDictIdMap.size();
    }

    public boolean isEmpty() {
      return _valueToDictIdMap.isEmpty();
    }

    public FloatSet keySet() {
      return _valueToDictIdMap.keySet();
    }
  }
}
