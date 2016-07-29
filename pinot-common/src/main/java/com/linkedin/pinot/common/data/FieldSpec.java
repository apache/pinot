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
import com.linkedin.pinot.common.utils.EqualityUtils;
import org.apache.avro.Schema.Type;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


/**
 * The <code>FieldSpec</code> class contains all specs related to any field (column) in {@link Schema}.
 * <p>There are 3 types of <code>FieldSpec</code>:
 * {@link DimensionFieldSpec}, {@link MetricFieldSpec}, {@link TimeFieldSpec}
 * <p>Specs stored are as followings:
 * <p>- <code>Name</code>: name of the field.
 * <p>- <code>DataType</code>: type of the data stored (e.g. INTEGER, LONG, FLOAT, DOUBLE, STRING).
 * <p>- <code>IsSingleValueField</code>: single-value or multi-value field.
 * <p>- <code>DefaultNullValue</code>: when no value found for this field, use this value. Stored in string format.
 */
public abstract class FieldSpec {
  private static final String DEFAULT_DIM_NULL_VALUE_OF_STRING = "null";
  private static final Integer DEFAULT_DIM_NULL_VALUE_OF_INT = Integer.MIN_VALUE;
  private static final Long DEFAULT_DIM_NULL_VALUE_OF_LONG = Long.MIN_VALUE;
  private static final Float DEFAULT_DIM_NULL_VALUE_OF_FLOAT = Float.NEGATIVE_INFINITY;
  private static final Double DEFAULT_DIM_NULL_VALUE_OF_DOUBLE = Double.NEGATIVE_INFINITY;

  private static final String DEFAULT_METRIC_NULL_VALUE_OF_STRING = "null";
  private static final Integer DEFAULT_METRIC_NULL_VALUE_OF_INT = 0;
  private static final Long DEFAULT_METRIC_NULL_VALUE_OF_LONG = 0L;
  private static final Float DEFAULT_METRIC_NULL_VALUE_OF_FLOAT = 0.0F;
  private static final Double DEFAULT_METRIC_NULL_VALUE_OF_DOUBLE = 0.0D;

  private String _name;
  private DataType _dataType;
  private boolean _isSingleValueField = true;
  private String _stringDefaultNullValue;
  private transient Object _cachedDefaultNullValue;

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public FieldSpec() {
  }

  public FieldSpec(String name, DataType dataType, boolean isSingleValueField) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(dataType);

    _name = name;
    _dataType = dataType;
    _isSingleValueField = isSingleValueField;
  }

  public abstract FieldType getFieldType();

  public void setName(String name) {
    Preconditions.checkNotNull(name);

    _name = name;
  }

  public String getName() {
    return _name;
  }

  public void setDataType(DataType dataType) {
    Preconditions.checkNotNull(dataType);

    _dataType = dataType;
    _cachedDefaultNullValue = null;
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

    _stringDefaultNullValue = defaultNullValue.toString();
    _cachedDefaultNullValue = null;
  }

  // BOOLEAN, BYTE, CHAR, SHORT support is a temporary work around, will remove it after using canonical schema.
  public Object getDefaultNullValue() {
    FieldType fieldType = getFieldType();
    if (_cachedDefaultNullValue == null) {
      if (_stringDefaultNullValue != null) {
        switch (_dataType) {
          case BYTE:
          case SHORT:
          case INT:
            _cachedDefaultNullValue = Integer.valueOf(_stringDefaultNullValue);
            break;
          case LONG:
            _cachedDefaultNullValue = Long.valueOf(_stringDefaultNullValue);
            break;
          case FLOAT:
            _cachedDefaultNullValue = Float.valueOf(_stringDefaultNullValue);
            break;
          case DOUBLE:
            _cachedDefaultNullValue = Double.valueOf(_stringDefaultNullValue);
            break;
          case BOOLEAN:
          case CHAR:
          case STRING:
            Preconditions.checkState(fieldType != FieldType.METRIC,
                "For metric field, no support for non-number data type: " + _dataType);
            _cachedDefaultNullValue = _stringDefaultNullValue;
            break;
          default:
            throw new UnsupportedOperationException("Unsupported data type: " + _dataType);
        }
      } else {
        switch (fieldType) {
          case METRIC:
            switch (_dataType) {
              case BYTE:
              case SHORT:
              case INT:
                _cachedDefaultNullValue = DEFAULT_METRIC_NULL_VALUE_OF_INT;
                break;
              case LONG:
                _cachedDefaultNullValue = DEFAULT_METRIC_NULL_VALUE_OF_LONG;
                break;
              case FLOAT:
                _cachedDefaultNullValue = DEFAULT_METRIC_NULL_VALUE_OF_FLOAT;
                break;
              case DOUBLE:
                _cachedDefaultNullValue = DEFAULT_METRIC_NULL_VALUE_OF_DOUBLE;
                break;
              case STRING:
                _cachedDefaultNullValue = DEFAULT_METRIC_NULL_VALUE_OF_STRING;
                break;
              default:
                throw new UnsupportedOperationException(
                    "Unknown default null value for metric field of data type: " + _dataType);
            }
            break;
          case DIMENSION:
          case TIME:
            switch (_dataType) {
              case BYTE:
              case SHORT:
              case INT:
                _cachedDefaultNullValue = DEFAULT_DIM_NULL_VALUE_OF_INT;
                break;
              case LONG:
                _cachedDefaultNullValue = DEFAULT_DIM_NULL_VALUE_OF_LONG;
                break;
              case FLOAT:
                _cachedDefaultNullValue = DEFAULT_DIM_NULL_VALUE_OF_FLOAT;
                break;
              case DOUBLE:
                _cachedDefaultNullValue = DEFAULT_DIM_NULL_VALUE_OF_DOUBLE;
                break;
              case BOOLEAN:
              case CHAR:
              case STRING:
                _cachedDefaultNullValue = DEFAULT_DIM_NULL_VALUE_OF_STRING;
                break;
              default:
                throw new UnsupportedOperationException(
                    "Unknown default null value for dimension/time field of data type: " + _dataType);
            }
            break;
          default:
            throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
        }
      }
    }
    return _cachedDefaultNullValue;
  }

  @Override
  public String toString() {
    return "< field type: " + getFieldType() + ", field name: " + _name + ", data type: " + _dataType
        + ", is single-value field: " + _isSingleValueField + ", default null value: " + getDefaultNullValue() + " >";
  }

  @Override
  public boolean equals(Object anObject) {
    if (this == anObject) {
      return true;
    }
    if (anObject instanceof FieldSpec) {
      FieldSpec anotherFieldSpec = (FieldSpec) anObject;

      return getFieldType().equals(anotherFieldSpec.getFieldType()) && _name.equals(anotherFieldSpec._name)
          && _dataType.equals(anotherFieldSpec._dataType) && _isSingleValueField == anotherFieldSpec._isSingleValueField
          && getDefaultNullValue().equals(anotherFieldSpec.getDefaultNullValue());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = getFieldType().hashCode();
    result = EqualityUtils.hashCodeOf(result, _name);
    result = EqualityUtils.hashCodeOf(result, _dataType);
    result = EqualityUtils.hashCodeOf(result, _isSingleValueField);
    result = EqualityUtils.hashCodeOf(result, getDefaultNullValue());
    return result;
  }

  /**
   * The <code>FieldType</code> enum is used to demonstrate the real world business logic for a column.
   * <p><code>DIMENSION</code>: columns used to filter records.
   * <p><code>METRIC</code>: columns used to apply aggregation on. <code>METRIC</code> field only contains numeric data.
   * <p><code>TIME</code>: time column (at most one per {@link Schema}). <code>TIME</code> field can be used to prune
   * segments, otherwise treated the same as <code>DIMENSION</code> field.
   */
  public enum FieldType {
    DIMENSION,
    METRIC,
    TIME
  }

  /**
   * The <code>DataType</code> enum is used to demonstrate the data type of a column.
   * <p>Array <code>DataType</code> is only used in {@link com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema}.
   * <p>In {@link Schema}, use non-array <code>DataType</code> only.
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

    /**
     * Return the {@link DataType} associate with the {@link Type}
     */
    public static DataType valueOf(Type avroType) {
      switch (avroType) {
        case INT:
          return INT;
        case LONG:
          return LONG;
        case FLOAT:
          return FLOAT;
        case DOUBLE:
          return DOUBLE;
        case BOOLEAN:
        case STRING:
        case ENUM:
          return STRING;
        default:
          throw new UnsupportedOperationException("Unsupported Avro type: " + avroType);
      }
    }

    /**
     * Return number of bytes needed for storage.
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
          throw new UnsupportedOperationException("Cannot get number of bytes for: " + this);
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
