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
