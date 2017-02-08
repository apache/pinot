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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectSet;


public class StringMutableDictionary extends MutableDictionaryReader {

  String2IntBiMap _dictionaryIdBiMap;
  private String _column;
  private String _min = null;
  private String _max = null;

  public StringMutableDictionary(String column) {
    _column = column;
    _dictionaryIdBiMap = new String2IntBiMap();
  }

  @Override
  public void index(Object rawValue) {
    if (rawValue instanceof Object[]) {
      for (Object o : (Object[]) rawValue) {
        _dictionaryIdBiMap.put(o.toString());
        updateMinMax(o.toString());
      }
    } else {
      _dictionaryIdBiMap.put(rawValue.toString());
      updateMinMax(rawValue.toString());
    }
  }

  private void updateMinMax(String entry) {
    if (_min == null && _max == null) {
      _min = entry;
      _max = entry;
      return;
    }

    if (entry.compareTo(_min) < 0) {
      _min = entry;
    } else if (entry.compareTo(_max) > 0) {
      _max = entry;
    }
  }

  @Override
  public boolean contains(Object rawValue) {
    if (rawValue == null) {
      return hasNull;
    }
    return _dictionaryIdBiMap.containsKey(rawValue.toString());
  }

  @Override
  public int indexOf(Object rawValue) {
    return _dictionaryIdBiMap.getValue(rawValue.toString());
  }

  @Override
  public Object get(int dictionaryId) {
    return _dictionaryIdBiMap.getKey(dictionaryId);
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return INVALID_ID;
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return INVALID_ID;
  }

  @Override
  public int getIntValue(int dictionaryId) {
    return INVALID_ID;
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    return INVALID_ID;
  }

  @Override
  public String toString(int dictionaryId) {
    return _dictionaryIdBiMap.getKey(dictionaryId);
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
  public String getStringValue(int dictionaryId) {
    return _dictionaryIdBiMap.getKey(dictionaryId);
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

  private String getString(int dictionaryId) {
    return _dictionaryIdBiMap.getKey(dictionaryId);
  }

  @Override
  public Object getMinVal() {
    return _min;
  }

  @Override
  public Object getMaxVal() {
    return _max;
  }

  public void print() {
    System.out.println("************* printing dictionary for column : " + _column + " ***************");
    for (String key : _dictionaryIdBiMap.keySet()) {
      System.out.println(key + "," + _dictionaryIdBiMap.getValue(key));
    }
    System.out.println("************************************");
  }

  public static class String2IntBiMap {
    private Object2IntMap<String> _valueToDictIdMap;
    private ObjectArrayList<String> _dictIdToValueMap;

    public String2IntBiMap() {
      _valueToDictIdMap = new Object2IntOpenHashMap<>();
      _valueToDictIdMap.defaultReturnValue(INVALID_ID);
      _dictIdToValueMap = new ObjectArrayList<>();
    }

    public void put(String value) {
      if (!_valueToDictIdMap.containsKey(value)) {
        _dictIdToValueMap.add(value);
        _valueToDictIdMap.put(value, _dictIdToValueMap.size() - 1);
      }
    }

    public String getKey(int dictId) {
      return (dictId < _dictIdToValueMap.size()) ? _dictIdToValueMap.get(dictId) : null;
    }

    public int getValue(String value) {
      return _valueToDictIdMap.getInt(value);
    }

    public boolean containsKey(String value) {
      return _valueToDictIdMap.containsKey(value);
    }

    public int size() {
      return _valueToDictIdMap.size();
    }

    public boolean isEmpty() {
      return _valueToDictIdMap.isEmpty();
    }

    public ObjectSet<String> keySet() {
      return _valueToDictIdMap.keySet();
    }
  }
}
