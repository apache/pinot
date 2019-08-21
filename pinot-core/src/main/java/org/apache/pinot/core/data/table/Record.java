package org.apache.pinot.core.data.table;

import java.util.Arrays;
import org.apache.pinot.common.utils.EqualityUtils;


public class Record {
  private Object[] _keys;
  private Object[] _values;

  public Record(Object[] keys, Object[] values) {
    _keys = keys;
    _values = values;
  }

  public Object[] getKeys() {
    return _keys;
  }

  public Object[] getValues() {
    return _values;
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    Record that = (Record) o;
    return Arrays.deepEquals(_keys, that._keys);
  }

  @Override
  public int hashCode() {
    return Arrays.deepHashCode(_keys);
  }
}
