package com.linkedin.pinot.common.data;

import org.apache.avro.Schema.Type;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public abstract class FieldSpec {
  String _name;
  FieldType _fieldType;
  DataType _dataType;
  boolean _isSingleValueField;
  String _delimiter;

  public FieldSpec() {

  }

  public FieldSpec(String name, FieldType fType, DataType dType, boolean singleValue, String delimeter) {
    _name = name;
    _fieldType = fType;
    _dataType = dType;
    _isSingleValueField = singleValue;
    _delimiter = delimeter;
  }

  public FieldSpec(String name, FieldType fType, DataType dType, boolean singleValue) {
    this(name, fType, dType, singleValue, null);
  }

  public FieldSpec(String name, FieldType fType, DataType dType, String delimeter) {
    this(name, fType, dType, false, delimeter);
  }

  public void setName(String name) {
    _name = name;
  }

  public String getName() {
    return _name;
  }

  public String getDelimiter() {
    return _delimiter;
  }

  public void setDelimeter(String delimeter) {
    _delimiter = delimeter;
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

  @Override
  public String toString() {
    return "< data type : " + _dataType + " , field type : " + _fieldType
        + ((_isSingleValueField) ? ", single value column" : ", multi value column") + ", delimeter : " + _delimiter
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
    BYTE,
    CHAR,
    SHORT,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    STRING,
    OBJECT,
    //EVERYTHING AFTER THIS MUST BE ARRAY TYPE
    BYTE_ARRAY,
    CHAR_ARRAY,
    SHORT_ARRAY,
    INT_ARRAY,
    LONG_ARRAY,
    FLOAT_ARRAY,
    DOUBLE_ARRAY,
    STRING_ARRAY;

    public boolean isSingleValue() {
      return this.ordinal() < BYTE_ARRAY.ordinal();
    }

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

    public JSONObject toJSONSchemaFor(String column) throws JSONException {
      final JSONObject ret = new JSONObject();
      ret.put("name", column);
      ret.put("doc", "data sample from load generator");
      switch (this) {
        case INT:
          final JSONArray intType = new JSONArray();
          intType.put("null");
          intType.put("int");
          ret.put("type", intType);
          return ret;
        case LONG:
          final JSONArray longType = new JSONArray();
          longType.put("null");
          longType.put("long");
          ret.put("type", longType);
          return ret;
        case FLOAT:
          final JSONArray floatType = new JSONArray();
          floatType.put("null");
          floatType.put("float");
          ret.put("type", floatType);
          return ret;
        case DOUBLE:
          final JSONArray doubleType = new JSONArray();
          doubleType.put("null");
          doubleType.put("double");
          ret.put("type", doubleType);
          return ret;
        case STRING:
          final JSONArray stringType = new JSONArray();
          stringType.put("null");
          stringType.put("string");
          ret.put("type", stringType);
          return ret;
        case BOOLEAN:
          final JSONArray booleanType = new JSONArray();
          booleanType.put("null");
          booleanType.put("boolean");
          ret.put("type", booleanType);
          return ret;
      }
      return null;
    }
  }
}
