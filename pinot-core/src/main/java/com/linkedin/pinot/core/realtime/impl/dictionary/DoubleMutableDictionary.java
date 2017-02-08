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

import it.unimi.dsi.fastutil.doubles.Double2IntMap;
import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleSet;


public class DoubleMutableDictionary extends MutableDictionaryReader {

  private Double2IntBiMap _dictionaryIdBiMap;
  private final String _column;
  private double _min = Double.MAX_VALUE;
  private double _max = Double.MIN_VALUE;

  public DoubleMutableDictionary(String column) {
    _column = column;
    _dictionaryIdBiMap = new Double2IntBiMap();
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
      Double entry = Double.parseDouble((String) value);
      _dictionaryIdBiMap.put(entry);
      updateMinMax(entry);
    } else if (value instanceof Double) {
      Double doubleValue = (Double) value;
      _dictionaryIdBiMap.put(doubleValue);
      updateMinMax(doubleValue);
    }
  }

  private void updateMinMax(double entry) {
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
      return _dictionaryIdBiMap.containsKey(Double.parseDouble(rawValue.toString()));
    } else if (rawValue instanceof Double) {
      return _dictionaryIdBiMap.containsKey((Double) rawValue);
    } else {
      throw new IllegalArgumentException(
          "Illegal argument type, expected double, got: " + rawValue.getClass().getName());
    }
  }

  @Override
  public int indexOf(Object rawValue) {
    if (rawValue instanceof String) {
      return _dictionaryIdBiMap.getValue(Double.parseDouble(rawValue.toString()));
    }
    return _dictionaryIdBiMap.getValue((Double) rawValue);
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
    return _dictionaryIdBiMap.getKey(dictionaryId);
  }

  @Override
  public int getIntValue(int dictionaryId) {
    return (_dictionaryIdBiMap.getKey(dictionaryId)).intValue();
  }

  @Override
  public float getFloatValue(int dictionaryId) {
    return _dictionaryIdBiMap.getKey(dictionaryId).floatValue();
  }

  @Override
  public String toString(int dictionaryId) {
    return String.valueOf(_dictionaryIdBiMap.getKey(dictionaryId));
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

    double lowerInDouble = Double.parseDouble(lower);
    double upperInDouble = Double.parseDouble(upper);

    double valueToCompare = _dictionaryIdBiMap.getKey(indexOfValueToCompare);

    boolean ret = true;

    if (includeLower) {
      if (valueToCompare < lowerInDouble) {
        ret = false;
      }
    } else {
      if (valueToCompare <= lowerInDouble) {
        ret = false;
      }
    }

    if (includeUpper) {
      if (valueToCompare > upperInDouble) {
        ret = false;
      }
    } else {
      if (valueToCompare >= upperInDouble) {
        ret = false;
      }
    }

    return ret;
  }

  @Override
  public String getStringValue(int dictionaryId) {
    return String.valueOf(_dictionaryIdBiMap.getKey(dictionaryId));
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
    for (double value : _dictionaryIdBiMap.keySet()) {
      System.out.println(value + "," + _dictionaryIdBiMap.getValue(value));
    }
    System.out.println("************************************");
  }

  public static class Double2IntBiMap {
    private Double2IntMap _valueToDictIdMap;
    private DoubleArrayList _dictIdToValueMap;

    public Double2IntBiMap() {
      _valueToDictIdMap = new Double2IntOpenHashMap();
      _dictIdToValueMap = new DoubleArrayList();
      _valueToDictIdMap.defaultReturnValue(INVALID_ID);
    }

    public void put(double value) {
      if (!_valueToDictIdMap.containsKey(value)) {
        _dictIdToValueMap.add(value);
        _valueToDictIdMap.put(value, _dictIdToValueMap.size() - 1);
      }
    }

    public Double getKey(int dictId) {
      return (dictId < _dictIdToValueMap.size()) ? _dictIdToValueMap.getDouble(dictId) : null;
    }

    public int getValue(double value) {
      return _valueToDictIdMap.get(value);
    }

    public boolean containsKey(double value) {
      return _valueToDictIdMap.containsKey(value);
    }

    public int size() {
      return _valueToDictIdMap.size();
    }

    public boolean isEmpty() {
      return _valueToDictIdMap.isEmpty();
    }

    public DoubleSet keySet() {
      return _valueToDictIdMap.keySet();
    }
  }
}
