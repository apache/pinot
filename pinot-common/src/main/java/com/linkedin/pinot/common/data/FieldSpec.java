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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.linkedin.pinot.common.utils.DataSchema;
import com.linkedin.pinot.common.utils.EqualityUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.Schema.Type;


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
@SuppressWarnings("unused")
public abstract class FieldSpec {
  private static final Integer DEFAULT_DIMENSION_NULL_VALUE_OF_INT = Integer.MIN_VALUE;
  private static final Long DEFAULT_DIMENSION_NULL_VALUE_OF_LONG = Long.MIN_VALUE;
  private static final Float DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT = Float.NEGATIVE_INFINITY;
  private static final Double DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE = Double.NEGATIVE_INFINITY;
  private static final String DEFAULT_DIMENSION_NULL_VALUE_OF_STRING = "null";

  private static final Integer DEFAULT_METRIC_NULL_VALUE_OF_INT = 0;
  private static final Long DEFAULT_METRIC_NULL_VALUE_OF_LONG = 0L;
  private static final Float DEFAULT_METRIC_NULL_VALUE_OF_FLOAT = 0.0F;
  private static final Double DEFAULT_METRIC_NULL_VALUE_OF_DOUBLE = 0.0D;
  private static final String DEFAULT_METRIC_NULL_VALUE_OF_STRING = "null";

  protected String _name;
  protected DataType _dataType;
  protected boolean _isSingleValueField = true;
  protected Object _defaultNullValue;

  private transient String _stringDefaultNullValue;

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public FieldSpec() {
  }

  public FieldSpec(@Nonnull String name, @Nonnull DataType dataType, boolean isSingleValueField) {
    _name = name;
    _dataType = dataType.getStoredType();
    _isSingleValueField = isSingleValueField;
    _defaultNullValue = getDefaultNullValue(getFieldType(), _dataType, null);
  }

  public FieldSpec(@Nonnull String name, @Nonnull DataType dataType, boolean isSingleValueField,
      @Nonnull Object defaultNullValue) {
    _name = name;
    _dataType = dataType.getStoredType();
    _isSingleValueField = isSingleValueField;
    _stringDefaultNullValue = defaultNullValue.toString();
    _defaultNullValue = getDefaultNullValue(getFieldType(), _dataType, _stringDefaultNullValue);
  }

  @Nonnull
  public abstract FieldType getFieldType();

  @Nonnull
  public String getName() {
    return _name;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setName(@Nonnull String name) {
    _name = name;
  }

  @Nonnull
  public DataType getDataType() {
    return _dataType;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setDataType(@Nonnull DataType dataType) {
    _dataType = dataType.getStoredType();
    _defaultNullValue = getDefaultNullValue(getFieldType(), _dataType, _stringDefaultNullValue);
  }

  public boolean isSingleValueField() {
    return _isSingleValueField;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setSingleValueField(boolean isSingleValueField) {
    _isSingleValueField = isSingleValueField;
  }

  @Nonnull
  public Object getDefaultNullValue() {
    return _defaultNullValue;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setDefaultNullValue(@Nonnull Object defaultNullValue) {
    _stringDefaultNullValue = defaultNullValue.toString();
    if (_dataType != null) {
      _defaultNullValue = getDefaultNullValue(getFieldType(), _dataType, _stringDefaultNullValue);
    }
  }

  private static Object getDefaultNullValue(@Nonnull FieldType fieldType, @Nonnull DataType dataType,
      @Nullable String stringDefaultNullValue) {
    if (stringDefaultNullValue != null) {
      switch (dataType) {
        case INT:
          return Integer.valueOf(stringDefaultNullValue);
        case LONG:
          return Long.valueOf(stringDefaultNullValue);
        case FLOAT:
          return Float.valueOf(stringDefaultNullValue);
        case DOUBLE:
          return Double.valueOf(stringDefaultNullValue);
        case STRING:
          return stringDefaultNullValue;
        default:
          throw new UnsupportedOperationException("Unsupported data type: " + dataType);
      }
    } else {
      switch (fieldType) {
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
            case STRING:
              return DEFAULT_METRIC_NULL_VALUE_OF_STRING;
            default:
              throw new UnsupportedOperationException(
                  "Unknown default null value for metric field of data type: " + dataType);
          }
        case DIMENSION:
        case TIME:
        case DATE_TIME:
          switch (dataType) {
            case INT:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_INT;
            case LONG:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_LONG;
            case FLOAT:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT;
            case DOUBLE:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE;
            case STRING:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
            default:
              throw new UnsupportedOperationException(
                  "Unknown default null value for dimension/time field of data type: " + dataType);
          }
        default:
          throw new UnsupportedOperationException("Unsupported field type: " + fieldType);
      }
    }
  }

  /**
   * Returns the {@link JsonObject} representing the field spec.
   * <p>Only contains fields with non-default value.
   * <p>NOTE: here we use {@link JsonObject} to preserve the insertion order.
   */
  @Nonnull
  public JsonObject toJsonObject() {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("name", _name);
    jsonObject.addProperty("dataType", _dataType.name());
    if (!_isSingleValueField) {
      jsonObject.addProperty("singleValueField", false);
    }
    appendDefaultNullValue(jsonObject);
    return jsonObject;
  }

  protected void appendDefaultNullValue(@Nonnull JsonObject jsonObject) {
    if (!_defaultNullValue.equals(getDefaultNullValue(getFieldType(), _dataType, null))) {
      if (_defaultNullValue instanceof Number) {
        jsonObject.add("defaultNullValue", new JsonPrimitive((Number) _defaultNullValue));
      } else {
        jsonObject.addProperty("defaultNullValue", _defaultNullValue.toString());
      }
    }
  }

  @Nonnull
  public JsonObject toAvroSchemaJsonObject() {
    JsonObject jsonSchema = new JsonObject();
    jsonSchema.addProperty("name", _name);
    switch (_dataType) {
      case INT:
        jsonSchema.add("type", convertStringsToJsonArray("null", "int"));
        return jsonSchema;
      case LONG:
        jsonSchema.add("type", convertStringsToJsonArray("null", "long"));
        return jsonSchema;
      case FLOAT:
        jsonSchema.add("type", convertStringsToJsonArray("null", "float"));
        return jsonSchema;
      case DOUBLE:
        jsonSchema.add("type", convertStringsToJsonArray("null", "double"));
        return jsonSchema;
      case STRING:
        jsonSchema.add("type", convertStringsToJsonArray("null", "string"));
        return jsonSchema;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private static JsonArray convertStringsToJsonArray(String... strings) {
    JsonArray jsonArray = new JsonArray();
    for (String string : strings) {
      jsonArray.add(new JsonPrimitive(string));
    }
    return jsonArray;
  }

  @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    FieldSpec that = (FieldSpec) o;
    return EqualityUtils.isEqual(_name, that._name) && EqualityUtils.isEqual(_dataType, that._dataType) && EqualityUtils
        .isEqual(_isSingleValueField, that._isSingleValueField) && EqualityUtils.isEqual(_defaultNullValue,
        that._defaultNullValue);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_name);
    result = EqualityUtils.hashCodeOf(result, _dataType);
    result = EqualityUtils.hashCodeOf(result, _isSingleValueField);
    result = EqualityUtils.hashCodeOf(result, _defaultNullValue);
    return result;
  }

  /**
   * The <code>FieldType</code> enum is used to demonstrate the real world business logic for a column.
   * <p><code>DIMENSION</code>: columns used to filter records.
   * <p><code>METRIC</code>: columns used to apply aggregation on. <code>METRIC</code> field only contains numeric data.
   * <p><code>TIME</code>: time column (at most one per {@link Schema}). <code>TIME</code> field can be used to prune
   * <p><code>DATE_TIME</code>: time column (at most one per {@link Schema}). <code>TIME</code> field can be used to prune
   * segments, otherwise treated the same as <code>DIMENSION</code> field.
   */
  public enum FieldType {
    DIMENSION,
    METRIC,
    TIME,
    DATE_TIME
  }

  /**
   * The <code>DataType</code> enum is used to demonstrate the data type of a column.
   * <p>Array <code>DataType</code> is only used in {@link DataSchema}.
   * <p>In {@link Schema}, use non-array <code>DataType</code> only.
   * <p>In pinot, we store data using 5 <code>DataType</code>s: INT, LONG, FLOAT, DOUBLE, STRING. All other
   * <code>DataType</code>s will be converted to one of them.
   */
  public enum DataType {
    BOOLEAN,      // Stored as STRING.
    BYTE,         // Stored as INT.
    CHAR,         // Stored as STRING.
    SHORT,        // Stored as INT.
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    STRING,
    OBJECT,       // Used in dataTable to transfer data structure.
    //EVERYTHING AFTER THIS MUST BE ARRAY TYPE
    BYTE_ARRAY,   // Unused.
    CHAR_ARRAY,   // Unused.
    SHORT_ARRAY,  // Unused.
    INT_ARRAY,
    LONG_ARRAY,
    FLOAT_ARRAY,
    DOUBLE_ARRAY,
    STRING_ARRAY;

    public boolean isNumber() {
      switch (this) {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
          return true;
        default:
          return false;
      }
    }

    public boolean isInteger() {
      switch (this) {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
          return true;
        default:
          return false;
      }
    }

    public boolean isSingleValue() {
      return this.ordinal() < BYTE_ARRAY.ordinal();
    }

    public DataType toMultiValue() {
      switch (this) {
        case BYTE:
          return BYTE_ARRAY;
        case CHAR:
          return CHAR_ARRAY;
        case INT:
          return INT_ARRAY;
        case LONG:
          return LONG_ARRAY;
        case FLOAT:
          return FLOAT_ARRAY;
        case DOUBLE:
          return DOUBLE_ARRAY;
        case STRING:
          return STRING_ARRAY;
        default:
          throw new UnsupportedOperationException("Unsupported toMultiValue for data type: " + this);
      }
    }

    public DataType toSingleValue() {
      switch (this) {
        case BYTE_ARRAY:
          return BYTE;
        case CHAR_ARRAY:
          return CHAR;
        case INT_ARRAY:
          return INT;
        case LONG_ARRAY:
          return LONG;
        case FLOAT_ARRAY:
          return FLOAT;
        case DOUBLE_ARRAY:
          return DOUBLE;
        case STRING_ARRAY:
          return STRING;
        default:
          throw new UnsupportedOperationException("Unsupported toSingleValue for data type: " + this);
      }
    }

    public boolean isCompatible(DataType anotherDataType) {
      // Single-value is not compatible with multi-value.
      if (isSingleValue() != anotherDataType.isSingleValue()) {
        return false;
      }
      // Number is not compatible with String.
      if (isSingleValue()) {
        return isNumber() == anotherDataType.isNumber();
      } else {
        return toSingleValue().isNumber() == anotherDataType.toSingleValue().isNumber();
      }
    }

    /**
     * Return the {@link DataType} stored in pinot.
     */
    public DataType getStoredType() {
      switch (this) {
        case BYTE:
        case SHORT:
        case INT:
          return INT;
        case LONG:
          return LONG;
        case FLOAT:
          return FLOAT;
        case DOUBLE:
          return DOUBLE;
        case BOOLEAN:
        case CHAR:
        case STRING:
          return STRING;
        default:
          throw new UnsupportedOperationException("Unsupported data type: " + this);
      }
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
  }
}
