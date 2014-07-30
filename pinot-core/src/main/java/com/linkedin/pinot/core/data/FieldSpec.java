package com.linkedin.pinot.core.data;

import org.apache.avro.Schema.Type;


public class FieldSpec {
  String _name;
  FieldType _fieldType;
  DataType _dataType;
  boolean _isSingleValueField;
  String _delimeter;

  public FieldSpec() {

  }

  public FieldSpec(String name, FieldType fType, DataType dType, boolean singleValue, String delimeter) {
    this._name = name;
    this._fieldType = fType;
    this._dataType = dType;
    this._isSingleValueField = singleValue;
    this._delimeter = delimeter;
  }

  public FieldSpec(String name, FieldType fType, DataType dType, boolean singleValue) {
    this(name, fType, dType, singleValue, null);
  }

  public FieldSpec(String name, FieldType fType, DataType dType, String delimeter) {
    this(name, fType, dType, false, delimeter);
  }

  public void setName(String name) {
    this._name = name;
  }

  public String getName() {
    return _name;
  }

  public String getDelimeter() {
    return _delimeter;
  }

  public void setDelimeter(String delimeter) {
    _delimeter = delimeter;
  }

  public FieldType getFieldType() {
    return _fieldType;
  }

  public void setFieldType(FieldType fieldType) {
    _fieldType = fieldType;
  }

  public DataType getDataType() {
    return _dataType;
  }

  public void setDataType(DataType dataType) {
    _dataType = dataType;
  }

  public boolean isSingleValueField() {
    return _isSingleValueField;
  }

  public void setSingleValueField(boolean isSingleValueField) {
    _isSingleValueField = isSingleValueField;
  }

  public String toString() {
    return "< data type : " + _dataType + " , field type : " + _fieldType
        + ((_isSingleValueField) ? ", single value column" : ", multi value column") + ", delimeter : " + _delimeter
        + " >";
  }

  /**
   * FieldType is used to demonstrate the real world business logic for a column.
   * 
   */
  public enum FieldType {
    unknown,
    dimension,
    metric,
    time
  }

  /**
   * DataType is used to demonstrate the data type of a column.
   * 
   */
  public enum DataType {
    BOOLEAN,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    STRING;

    public static DataType valueOf(Type type) {
      if (type == Type.INT) {
        return INT;
      }
      if (type == Type.LONG) {
        return LONG;
      }

      if (type == Type.STRING || type == Type.BOOLEAN) {
        return STRING;
      }

      if (type == Type.DOUBLE || type == Type.FLOAT) {
        return FLOAT;
      }

      throw new UnsupportedOperationException(type.toString());
    }
  }
}
