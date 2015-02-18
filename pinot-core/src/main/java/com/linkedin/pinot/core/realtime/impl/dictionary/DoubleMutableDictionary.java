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
