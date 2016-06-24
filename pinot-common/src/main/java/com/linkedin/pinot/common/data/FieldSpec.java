/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.base.Preconditions;
import org.apache.avro.Schema.Type;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public abstract class FieldSpec {
  private static final String DEFAULT_DIM_NULL_VALUE_OF_STRING = "null";
  private static final Integer DEFAULT_DIM_NULL_VALUE_OF_INT = Integer.MIN_VALUE;
  private static final Long DEFAULT_DIM_NULL_VALUE_OF_LONG = Long.MIN_VALUE;
  private static final Float DEFAULT_DIM_NULL_VALUE_OF_FLOAT = Float.NEGATIVE_INFINITY;
  private static final Double DEFAULT_DIM_NULL_VALUE_OF_DOUBLE = Double.NEGATIVE_INFINITY;

  private static final Integer DEFAULT_METRIC_NULL_VALUE_OF_INT = 0;
  private static final Long DEFAULT_METRIC_NULL_VALUE_OF_LONG = 0L;
  private static final Float DEFAULT_METRIC_NULL_VALUE_OF_FLOAT = 0.0F;
  private static final Double DEFAULT_METRIC_NULL_VALUE_OF_DOUBLE = 0.0D;

  private String _name;
  private FieldType _fieldType;
  private DataType _dataType;
  private boolean _isSingleValueField = true;
  private String _delimiter = ",";
  private Object _defaultNullValue;

  // Fields to help make sure we first set data type then default null value.
  @JsonIgnore
  protected boolean _isDataTypeSet = false;
  @JsonIgnore
  protected boolean _isDefaultNullValueSet = false;

  public FieldSpec() {
  }

  public FieldSpec(String name, FieldType fieldType, DataType dataType, boolean isSingleValueField, String delimiter,
      Object defaultNullValue) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(fieldType);
    Preconditions.checkNotNull(dataType);

    _name = name;
    _fieldType = fieldType;
    _dataType = dataType;
    _isDataTypeSet = true;
    _isSingleValueField = isSingleValueField;
    _delimiter = delimiter;
    if (defaultNullValue != null) {
      setDefaultNullValue(defaultNullValue);
    }
  }

  public FieldSpec(String name, FieldType fieldType, DataType dataType, boolean isSingleValueField, String delimiter) {
    this(name, fieldType, dataType, isSingleValueField, delimiter, null);
  }

  public FieldSpec(String name, FieldType fieldType, DataType dataType, boolean isSingleValueField) {
    this(name, fieldType, dataType, isSingleValueField, null, null);
  }

  public void setName(String name) {
    Preconditions.checkNotNull(name);

    _name = name;
  }

  public String getName() {
    return _name;
  }

  public void setDelimiter(String delimiter) {
    _delimiter = delimiter;
  }

  public String getDelimiter() {
    return _delimiter;
  }

  public void setFieldType(FieldType fieldType) {
    Preconditions.checkNotNull(fieldType);

    _fieldType = fieldType;
  }

  public FieldType getFieldType() {
    return _fieldType;
  }

  public void setDataType(DataType dataType) {
    Preconditions.checkNotNull(dataType);

    _isDataTypeSet = true;
    _dataType = dataType;

    // Reset default null value if already set.
    if (_isDefaultNullValueSet) {
      updateDefaultNullValue();
    }
  }

  public DataType getDataType() {
    return _dataType;
  }

  public void setSingleValueField(boolean isSingleValueField) {
    _isSingleValueField = isSingleValueField;
  }

  public boolean isSingleValueField() {
    return _isSingleValueField;
  }

  public void setDefaultNullValue(Object defaultNullValue) {
    Preconditions.checkNotNull(defaultNullValue);

    _isDefaultNullValueSet = true;

    if (_isDataTypeSet) {
      defaultNullValueFromString(defaultNullValue.toString());
    } else {
      // Set default null value as the value passed in, and might reset when setDataType() get called.
      _defaultNullValue = defaultNullValue;
    }
  }

  /**
   * Helper function to convert the string format default null value to the correct data type.
   *
   * @param value string format of the default null value.
   */
  protected void defaultNullValueFromString(String value) {
    DataType dataType = getDataType();
    switch (dataType) {
      case INT:
        _defaultNullValue = Integer.valueOf(value);
        return;
      case LONG:
        _defaultNullValue = Long.valueOf(value);
        return;
      case FLOAT:
        _defaultNullValue = Float.valueOf(value);
        return;
      case DOUBLE:
        _defaultNullValue = Double.valueOf(value);
        return;
      case STRING:
        _defaultNullValue = value;
        return;
      default:
        throw new UnsupportedOperationException("Unknown data type " + dataType);
    }
  }

  /**
   * Helper function to update default null value according to the data type.
   */
  protected void updateDefaultNullValue() {
    defaultNullValueFromString(_defaultNullValue.toString());
  }

  public Object getDefaultNullValue() {
    if (_defaultNullValue != null) {
      return _defaultNullValue;
    }
    DataType dataType = getDataType();
    switch (_fieldType) {
      case METRIC:
        switch (dataType) {
          case INT:
            return DEFAULT_METRIC_NULL_VALUE_OF_INT;
          case LONG:
            return DEFAULT_METRIC_NULL_VALUE_OF_LONG;
          case FLOAT:
            return DEFAULT_METRIC_NULL_VALUE_OF_FLOAT;
          case DOUBLE:
            return DEFAULT_METRIC_NULL_VALUE_OF_DOUBLE;
          default:
            throw new UnsupportedOperationException("Unknown default null value for metric of data type " + _dataType);
        }
      case DIMENSION:
      case TIME:
        switch (dataType) {
          case INT:
            return DEFAULT_DIM_NULL_VALUE_OF_INT;
          case LONG:
            return DEFAULT_DIM_NULL_VALUE_OF_LONG;
          case FLOAT:
            return DEFAULT_DIM_NULL_VALUE_OF_FLOAT;
          case DOUBLE:
            return DEFAULT_DIM_NULL_VALUE_OF_DOUBLE;
          case STRING:
            return DEFAULT_DIM_NULL_VALUE_OF_STRING;
          default:
            throw new UnsupportedOperationException(
                "Unknown default null value for dimension/time column of data type " + _dataType);
        }
      default:
        throw new UnsupportedOperationException("Unknown field type" + _fieldType);
    }
  }

  @Override
  public String toString() {
    return "< data type: " + getDataType() + " , field type: " + getFieldType()
        + (isSingleValueField() ? ", single value column" : ", multi value column, delimiter: '" + getDelimiter() + "'")
        + ", default null value: " + getDefaultNullValue() + " >";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FieldSpec) {
      return toString().equals(obj.toString());
    }
    return false;
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
      return (this == BYTE) || (this == SHORT) || (this == INT) || (this == LONG) || (this == FLOAT)
          || (this == DOUBLE);
    }

    public boolean isInteger() {
      return (this == BYTE) || (this == SHORT) || (this == INT) || (this == LONG);
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

      if (type == Type.ENUM || type == Type.STRING || type == Type.BOOLEAN) {
        return STRING;
      }

      if (type == Type.FLOAT) {
        return FLOAT;
      }

      if (type == Type.DOUBLE) {
        return DOUBLE;
      }

      throw new UnsupportedOperationException("Unsupported DataType " + type.toString());
    }

    /**
     * return the number of bytes
     * @return
     */
    public int size() {
      switch (this) {
        case BYTE:
          return 1;
        case SHORT:
          return 2;
        case INT:
          return 4;
        case LONG:
          return 8;
        case FLOAT:
          return 4;
        case DOUBLE:
          return 8;
        default:
          throw new UnsupportedOperationException("Cant get number of bytes for :" + this);
      }
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
}
