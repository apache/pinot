package com.linkedin.pinot.core.realtime.impl.dictionary;

import com.linkedin.pinot.common.data.FieldSpec;


public class IntMutableDictionary extends MutableDictionaryReader {

  public IntMutableDictionary(FieldSpec spec) {
    super(spec);
  }

  @Override
  public void index(Object rawValue) {

    if (rawValue == null)
      return;

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
  public int indexOf(Object rawValue) {

    if (rawValue == null)
      return 0;

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
    return ((Integer)getRawValueFromBiMap(dictionaryId)).longValue();
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return new Double(((Integer)getRawValueFromBiMap(dictionaryId)).intValue());
  }

  @Override
  public String toString(int dictionaryId) {
    return ((Integer)getRawValueFromBiMap(dictionaryId)).toString();
  }

  @Override
  public boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper) {

    int lowerInInt = Integer.parseInt(lower);
    int upperInInt = Integer.parseInt(upper);

    int valueToCompare = getInt(indexOfValueToCompare);

    boolean ret = true;

    if (includeLower) {
      if (valueToCompare < lowerInInt )
        ret = false;
    } else {
      if (valueToCompare <= lowerInInt)
        ret = false;
    }

    if (includeUpper) {
      if (valueToCompare > upperInInt )
        ret = false;
    } else {
      if (valueToCompare >= upperInInt)
        ret = false;
    }

    return ret;
  }

}
