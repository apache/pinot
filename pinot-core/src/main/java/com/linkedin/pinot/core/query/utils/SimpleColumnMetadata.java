package com.linkedin.pinot.core.query.utils;

import org.apache.commons.configuration.Configuration;


public class SimpleColumnMetadata {
  private String _name;
  private String _columnType;
  private int _size;

  public SimpleColumnMetadata(String name, String type) {
    _name = name;
    _columnType = type;
  }

  public SimpleColumnMetadata() {

  }

  public void addToConfig(Configuration configuration) {
    configuration.setProperty("column." + _name + ".columnType", _columnType);
    configuration.setProperty("column." + _name + ".size", _size);
  }

  public String getName() {
    return _name;
  }

  public void setName(String name) {
    this._name = name;
  }

  public String getColumnType() {
    return _columnType;
  }

  public void setColumnType(String originalType) {
    this._columnType = originalType;
  }

  @Override
  public String toString() {
    return "ColumnMetadata [_name=" + _name + ", _columnType=" + _columnType + ", _size=" + _size + "]";
  }

  public static SimpleColumnMetadata valueOf(Configuration config, String columnName) {
    SimpleColumnMetadata metadata = new SimpleColumnMetadata();
    metadata.setSize(config.getInt("column." + columnName + ".size"));
    metadata.setColumnType(config.getString("column." + columnName + ".columnType"));
    metadata.setName(columnName);

    return metadata;
  }

  public void setSize(int size) {
    this._size = size;
  }

  public int getSize() {
    return _size;
  }
}
