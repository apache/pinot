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

import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongSet;


public class LongMutableDictionary extends MutableDictionaryReader {

  private Long2IntBiMap _dictionaryIdBiMap;
  private String _column;
  private long _min = Long.MAX_VALUE;
  private long _max = Long.MIN_VALUE;

  public LongMutableDictionary(String column) {
    _column = column;
    _dictionaryIdBiMap = new Long2IntBiMap();
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
      Long e = Long.parseLong((String) value);
      _dictionaryIdBiMap.put(e);
      updateMinMax(e);
    } else if (value instanceof Long) {
      Long longValue = (Long) value;
      _dictionaryIdBiMap.put(longValue);
      updateMinMax(longValue);
    }
  }

  private void updateMinMax(Long entry) {
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
      return _dictionaryIdBiMap.containsKey(Long.parseLong((String) rawValue));
    }
    return _dictionaryIdBiMap.containsKey((Long) rawValue);
  }

  @Override
  public int indexOf(Object rawValue) {
    if (rawValue instanceof String) {
      return _dictionaryIdBiMap.getValue(Long.parseLong(rawValue.toString()));
    }
    return _dictionaryIdBiMap.getValue((Long) rawValue);
  }

  @Override
  public Object get(int dictionaryId) {
    return _dictionaryIdBiMap.getKey(dictionaryId);
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return _dictionaryIdBiMap.getKey(dictionaryId);
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
  public boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper) {

    long lowerInLong = Long.parseLong(lower);
    long upperInLong = Long.parseLong(upper);

    long valueToCompare = _dictionaryIdBiMap.getKey(indexOfValueToCompare);

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

  public void print() {
    System.out.println("************* printing dictionary for column : " + _column + " ***************");
    for (long key : _dictionaryIdBiMap.keySet()) {
      System.out.println(key + "," + _dictionaryIdBiMap.getValue(key));
    }
    System.out.println("************************************");
  }

  public static class Long2IntBiMap {
    private Long2IntMap _valueToDictIdMap;
    private LongArrayList _dictIdToValueMap;

    public Long2IntBiMap() {
      _valueToDictIdMap = new Long2IntOpenHashMap();
      _dictIdToValueMap = new LongArrayList();
      _valueToDictIdMap.defaultReturnValue(INVALID_ID);
    }

    public void put(long value) {
      if (!_valueToDictIdMap.containsKey(value)) {
        _dictIdToValueMap.add(value);
        _valueToDictIdMap.put(value, _dictIdToValueMap.size() - 1);
      }
    }

    public Long getKey(int dictId) {
      return (dictId < _dictIdToValueMap.size()) ? _dictIdToValueMap.getLong(dictId) : null;
    }

    public int getValue(long value) {
      return _valueToDictIdMap.get(value);
    }

    public boolean containsKey(long value) {
      return _valueToDictIdMap.containsKey(value);
    }

    public int size() {
      return _valueToDictIdMap.size();
    }

    public boolean isEmpty() {
      return _valueToDictIdMap.isEmpty();
    }

    public LongSet keySet() {
      return _valueToDictIdMap.keySet();
    }
  }
}
