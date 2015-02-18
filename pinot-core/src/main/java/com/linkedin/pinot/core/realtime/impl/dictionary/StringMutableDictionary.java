package com.linkedin.pinot.core.realtime.impl.dictionary;

import com.linkedin.pinot.common.data.FieldSpec;


public class StringMutableDictionary extends MutableDictionaryReader {

  public StringMutableDictionary(FieldSpec spec) {
    super(spec);
  }

  @Override
  public void index(Object rawValue) {
    if (rawValue == null)
      return;

    if (rawValue instanceof String) {
      addToDictionaryBiMap(rawValue.toString());
      return;
    }

    if (rawValue instanceof Object[]) {
      for (Object o : (Object[]) rawValue) {
        if (o instanceof String) {
          addToDictionaryBiMap(o.toString());
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
      return getIndexOfFromBiMap(rawValue.toString());
    }

    return getIndexOfFromBiMap(rawValue);
  }

  @Override
  public Object get(int dictionaryId) {
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

}
