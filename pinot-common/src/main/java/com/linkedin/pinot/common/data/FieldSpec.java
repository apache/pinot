/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.data;

import org.apache.avro.Schema.Type;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


// Json annotation required for abstract classes.
//@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "type")
public abstract class FieldSpec {
  private static final String DEFAULT_DIM_NULL_VALUE_OF_STRING = "null";
  private static final Integer DEFAULT_DIM_NULL_VALUE_OF_INT = Integer.valueOf(Integer.MIN_VALUE);
  private static final Long DEFAULT_DIM_NULL_VALUE_OF_LONG = Long.valueOf(Long.MIN_VALUE);
  private static final Float DEFAULT_DIM_NULL_VALUE_OF_FLOAT = Float.valueOf(Float.NEGATIVE_INFINITY);
  private static final Double DEFAULT_DIM_NULL_VALUE_OF_DOUBLE = Double.valueOf(Double.NEGATIVE_INFINITY);

  private static final Integer DEFAULT_METRIC_NULL_VALUE_OF_INT = Integer.valueOf(0);
  private static final Long DEFAULT_METRIC_NULL_VALUE_OF_LONG = Long.valueOf(0);
  private static final Float DEFAULT_METRIC_NULL_VALUE_OF_FLOAT = Float.valueOf(0);
  private static final Double DEFAULT_METRIC_NULL_VALUE_OF_DOUBLE = Double.valueOf(0);

  String name;
  FieldType fieldType;
  DataType dataType;
  boolean isSingleValueField = true;
  String delimiter = ",";
  Object defaultNullValue;

  public FieldSpec() {

  }

  public FieldSpec(String name, FieldType fType, DataType dType, boolean singleValue, String delimeter) {
    this.name = name;
    this.fieldType = fType;
    this.dataType = dType;
    this.isSingleValueField = singleValue;
    this.delimiter = delimeter;
  }

  public FieldSpec(String name, FieldType fType, DataType dType, boolean singleValue, String delimeter,
      Object defaultNullValue) {
    this.name = name;
    fieldType = fType;
    dataType = dType;
    isSingleValueField = singleValue;
    delimiter = delimeter;
    this.defaultNullValue = defaultNullValue;
  }

  public FieldSpec(String name, FieldType fType, DataType dType, boolean singleValue) {
    this(name, fType, dType, singleValue, null);
  }

  public FieldSpec(String name, FieldType fType, DataType dType, String delimeter) {
    this(name, fType, dType, false, delimeter);
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public void setDelimiter(String delimeter) {
    delimiter = delimeter;
  }

  public FieldType getFieldType() {
    return fieldType;
  }

  public void setFieldType(FieldType fieldType) {
    this.fieldType = fieldType;
  }

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  public boolean isSingleValueField() {
    return isSingleValueField;
  }

  public void setSingleValueField(boolean isSingleValueField) {
    this.isSingleValueField = isSingleValueField;
  }

  public void setDefaultNullValue(Object defaultNullValue) {
    this.defaultNullValue = defaultNullValue;
  }

  @Override
  public String toString() {
    return "< data type : " + dataType + " , field type : " + fieldType
        + ((isSingleValueField) ? ", single value column" : ", multi value column") + ", delimeter : " + delimiter
        + " >";
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    return this.toString().equals(other.toString());
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  /**
   * FieldType is used to demonstrate the real world business logic for a column.
   *
   */
  public enum FieldType {
    UNKNOWN,
    DIMENSION,
    METRIC,
    TIME
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

    public boolean isNumber() {
      return (this == BYTE) || (this == SHORT) || (this == INT) || (this == LONG) || (this == FLOAT) || (this == DOUBLE);
    }

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

      if (type == Type.FLOAT) {
        return FLOAT;
      }

      if (type == Type.DOUBLE) {
        return DOUBLE;
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
        default:
          return null;
      }
    }
  }

  public Object getDefaultNullValue() {
    if (defaultNullValue != null) {
      return defaultNullValue;
    }
    switch (getFieldType()) {
      case METRIC:
        switch (dataType) {
          case INT:
          case INT_ARRAY:
            return DEFAULT_METRIC_NULL_VALUE_OF_INT;
          case LONG:
          case LONG_ARRAY:
            return DEFAULT_METRIC_NULL_VALUE_OF_LONG;
          case FLOAT:
          case FLOAT_ARRAY:
            return DEFAULT_METRIC_NULL_VALUE_OF_FLOAT;
          case DOUBLE:
          case DOUBLE_ARRAY:
            return DEFAULT_METRIC_NULL_VALUE_OF_DOUBLE;
          default:
            throw new UnsupportedOperationException("Unknown default null value for metric of data type " + dataType);
        }
      case DIMENSION:
      case TIME:
        switch (getDataType()) {
          case INT:
          case INT_ARRAY:
            return DEFAULT_DIM_NULL_VALUE_OF_INT;
          case LONG:
          case LONG_ARRAY:
            return DEFAULT_DIM_NULL_VALUE_OF_LONG;
          case FLOAT:
          case FLOAT_ARRAY:
            return DEFAULT_DIM_NULL_VALUE_OF_FLOAT;
          case DOUBLE:
          case DOUBLE_ARRAY:
            return DEFAULT_DIM_NULL_VALUE_OF_DOUBLE;
          case STRING:
          case STRING_ARRAY:
            return DEFAULT_DIM_NULL_VALUE_OF_STRING;
          default:
            throw new UnsupportedOperationException("Unknown default null value for dimension/time column of data type " + dataType);
        }
      default:
        throw new UnsupportedOperationException("Not supported data type for null value - " + dataType);
    }
  }
}
