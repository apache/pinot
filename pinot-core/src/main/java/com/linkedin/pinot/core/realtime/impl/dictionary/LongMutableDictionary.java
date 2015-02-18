package com.linkedin.pinot.core.realtime.impl.dictionary;

import com.linkedin.pinot.common.data.FieldSpec;


public class LongMutableDictionary extends MutableDictionaryReader {

  public LongMutableDictionary(FieldSpec spec) {
    super(spec);
  }

  @Override
  public void index(Object rawValue) {
    if (rawValue == null)
      return;

    if (rawValue instanceof String) {
      addToDictionaryBiMap(new Long(Long.parseLong(rawValue.toString())));
      return;
    }

    if (rawValue instanceof Long) {
      addToDictionaryBiMap(rawValue);
      return;
    }

    if (rawValue instanceof Object[]) {
      for (Object o : (Object[]) rawValue) {
        if (o instanceof String) {
          addToDictionaryBiMap(new Long(Long.parseLong(o.toString())));
          continue;
        }

        if (o instanceof Long) {
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
      return getIndexOfFromBiMap(new Long(Long.parseLong(rawValue.toString())));
    }

    return getIndexOfFromBiMap(rawValue);
  }

  @Override
  public Object get(int dictionaryId) {
    return getRawValueFromBiMap(dictionaryId);
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return ((Long) getRawValueFromBiMap(dictionaryId)).longValue();
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return new Double(((Long) getRawValueFromBiMap(dictionaryId)).longValue());
  }

  @Override
  public String toString(int dictionaryId) {
    return ((Long) getRawValueFromBiMap(dictionaryId)).toString();
  }

  @Override
  public boolean inRange(String lower, String upper, int indexOfValueToCompare, boolean includeLower,
      boolean includeUpper) {

    long lowerInLong = Long.parseLong(lower);
    long upperInLong = Long.parseLong(upper);

    long valueToCompare = getLong(indexOfValueToCompare);

    boolean ret = true;

    if (includeLower) {
      if (valueToCompare < lowerInLong)
        ret = false;
    } else {
      if (valueToCompare <= lowerInLong)
        ret = false;
    }

    if (includeUpper) {
      if (valueToCompare > upperInLong)
        ret = false;
    } else {
      if (valueToCompare >= upperInLong)
        ret = false;
    }

    return ret;
  }

}
