package org.apache.pinot.core.data.table;

import java.util.Arrays;


/**
 * Defines a single record in Pinot comprising of keys and values
 */
public class Record {
  private Object[] _keys;
  private Object[] _values;

  public Record(Object[] keys, Object[] values) {
    _keys = keys;
    _values = values;
  }

  /**
   * Gets the key portion of the record
   */
  public Object[] getKeys() {
    return _keys;
  }

  /**
   * Gets the values portion of the record
   */
  public Object[] getValues() {
    return _values;
  }

  @Override
  public boolean equals(Object o) {
    Record that = (Record) o;
    return Arrays.deepEquals(_keys, that._keys);
  }

  @Override
  public int hashCode() {
    return Arrays.deepHashCode(_keys);
  }
}
