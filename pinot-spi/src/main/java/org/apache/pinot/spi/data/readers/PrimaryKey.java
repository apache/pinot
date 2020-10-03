package org.apache.pinot.spi.data.readers;

import java.util.Arrays;


/**
 * The primary key of a record.
 */
public class PrimaryKey {
  private final Object[] _fields;

  public PrimaryKey(Object[] fields) {
    _fields = fields;
  }

  public Object[] getFields() {
    return _fields;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof PrimaryKey) {
      PrimaryKey that = (PrimaryKey) obj;
      return Arrays.equals(_fields, that._fields);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(_fields);
  }

  @Override
  public String toString() {
    return Arrays.toString(_fields);
  }
}
