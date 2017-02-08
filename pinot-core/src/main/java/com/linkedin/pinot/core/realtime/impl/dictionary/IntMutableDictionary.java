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

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntSet;


public class IntMutableDictionary extends MutableDictionaryReader {

  private Int2IntBiMap _dictionaryIdBiMap;
  private String _column;
  private int _min = Integer.MAX_VALUE;
  private int _max = Integer.MIN_VALUE;

  public IntMutableDictionary(String column) {
    _column = column;
    _dictionaryIdBiMap = new Int2IntBiMap();
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
      Integer entry = Integer.parseInt((String) value);
      _dictionaryIdBiMap.put(entry);
      updateMinMax(entry);
    } else if (value instanceof Integer) {
      Integer intValue = (Integer) value;
      _dictionaryIdBiMap.put(intValue);
      updateMinMax(intValue);
    }
  }

  private void updateMinMax(Integer entry) {
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
      return _dictionaryIdBiMap.containsKey(Integer.parseInt(rawValue.toString()));
    }
    return _dictionaryIdBiMap.containsKey((Integer) rawValue);
  }

  @Override
  public int indexOf(Object rawValue) {
    if (rawValue instanceof String) {
      return _dictionaryIdBiMap.getValue(Integer.parseInt(rawValue.toString()));
    }
    return _dictionaryIdBiMap.getValue((Integer) rawValue);
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
    return _dictionaryIdBiMap.getKey(dictionaryId);
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    return (_dictionaryIdBiMap.getKey(dictionaryId)).floatValue();
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
  public void readIntValues(int[] dictionaryIds, int startPos, int limit, int[] outValues, int outStartPos) {
    int endPos = startPos + limit;
    for (int iter = startPos; iter < endPos; ++iter) {
      int dictId = dictionaryIds[iter];
      outValues[outStartPos++] = _dictionaryIdBiMap.getKey(dictId);
    }
  }

  @Override
  public String getStringValue(int dictionaryId) {
    return (_dictionaryIdBiMap.getKey(dictionaryId)).toString();
  }

  @Override
  public boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper) {

    int lowerInInt = Integer.parseInt(lower);
    int upperInInt = Integer.parseInt(upper);

    int valueToCompare = _dictionaryIdBiMap.getKey(indexOfValueToCompare);

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
    for (int value : _dictionaryIdBiMap.keySet()) {
      System.out.println(value + "," + _dictionaryIdBiMap.getValue(value));
    }
    System.out.println("************************************");
  }

  public static class Int2IntBiMap {
    private Int2IntMap _valueToDictIdMap;
    private IntArrayList _dictIdToValueMap;

    public Int2IntBiMap() {
      _valueToDictIdMap = new Int2IntOpenHashMap();
      _valueToDictIdMap.defaultReturnValue(INVALID_ID);
      _dictIdToValueMap = new IntArrayList();
    }

    public void put(int value) {
      if (!_valueToDictIdMap.containsKey(value)) {
        _dictIdToValueMap.add(value);
        _valueToDictIdMap.put(value, _dictIdToValueMap.size() - 1);
      }
    }

    public Integer getKey(int dictId) {
      return (dictId < _dictIdToValueMap.size()) ? _dictIdToValueMap.getInt(dictId) : null;
    }

    public int getValue(int value) {
      return _valueToDictIdMap.get(value);
    }

    public boolean containsKey(int value) {
      return _valueToDictIdMap.containsKey(value);
    }

    public int size() {
      return _valueToDictIdMap.size();
    }

    public boolean isEmpty() {
      return _valueToDictIdMap.isEmpty();
    }

    public IntSet keySet() {
      return _valueToDictIdMap.keySet();
    }
  }
}
