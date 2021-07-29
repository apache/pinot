/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.Serializable;
import java.sql.Timestamp;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimestampUtils;


/**
 * The <code>FieldSpec</code> class contains all specs related to any field (column) in {@link Schema}.
 * <p>There are 3 types of <code>FieldSpec</code>:
 * {@link DimensionFieldSpec}, {@link MetricFieldSpec}, {@link TimeFieldSpec}
 * <p>Specs stored are as followings:
 * <p>- <code>Name</code>: name of the field.
 * <p>- <code>DataType</code>: type of the data stored (e.g. INTEGER, LONG, FLOAT, DOUBLE, STRING).
 * <p>- <code>IsSingleValueField</code>: single-value or multi-value field.
 * <p>- <code>DefaultNullValue</code>: when no value found for this field, use this value. Stored in string format.
 * <p>- <code>VirtualColumnProvider</code>: the virtual column provider to use for this field.
 */
@SuppressWarnings("unused")
public abstract class FieldSpec implements Comparable<FieldSpec>, Serializable {
  public static final int DEFAULT_MAX_LENGTH = 512;

  public static final Integer DEFAULT_DIMENSION_NULL_VALUE_OF_INT = Integer.MIN_VALUE;
  public static final Long DEFAULT_DIMENSION_NULL_VALUE_OF_LONG = Long.MIN_VALUE;
  public static final Float DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT = Float.NEGATIVE_INFINITY;
  public static final Double DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE = Double.NEGATIVE_INFINITY;
  public static final Integer DEFAULT_DIMENSION_NULL_VALUE_OF_BOOLEAN = 0;
  public static final Long DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP = 0L;
  public static final String DEFAULT_DIMENSION_NULL_VALUE_OF_STRING = "null";
  public static final String DEFAULT_DIMENSION_NULL_VALUE_OF_JSON = "null";
  public static final byte[] DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES = new byte[0];

  public static final Integer DEFAULT_METRIC_NULL_VALUE_OF_INT = 0;
  public static final Long DEFAULT_METRIC_NULL_VALUE_OF_LONG = 0L;
  public static final Float DEFAULT_METRIC_NULL_VALUE_OF_FLOAT = 0.0F;
  public static final Double DEFAULT_METRIC_NULL_VALUE_OF_DOUBLE = 0.0D;
  public static final String DEFAULT_METRIC_NULL_VALUE_OF_STRING = "null";
  public static final byte[] DEFAULT_METRIC_NULL_VALUE_OF_BYTES = new byte[0];

  protected String _name;
  protected DataType _dataType;
  protected boolean _isSingleValueField = true;

  // NOTE: for STRING column, this is the max number of characters; for BYTES column, this is the max number of bytes
  private int _maxLength = DEFAULT_MAX_LENGTH;

  protected Object _defaultNullValue;
  private transient String _stringDefaultNullValue;

  // Transform function to generate this column, can be based on other columns
  @Deprecated // Set this in TableConfig -> IngestionConfig -> TransformConfigs
  protected String _transformFunction;

  protected String _virtualColumnProvider;

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public FieldSpec() {
  }

  public FieldSpec(String name, DataType dataType, boolean isSingleValueField) {
    this(name, dataType, isSingleValueField, DEFAULT_MAX_LENGTH, null);
  }

  public FieldSpec(String name, DataType dataType, boolean isSingleValueField, @Nullable Object defaultNullValue) {
    this(name, dataType, isSingleValueField, DEFAULT_MAX_LENGTH, defaultNullValue);
  }

  public FieldSpec(String name, DataType dataType, boolean isSingleValueField, int maxLength,
      @Nullable Object defaultNullValue) {
    _name = name;
    _dataType = dataType;
    _isSingleValueField = isSingleValueField;
    _maxLength = maxLength;
    setDefaultNullValue(defaultNullValue);
  }

  public abstract FieldType getFieldType();

  public String getName() {
    return _name;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setName(String name) {
    _name = name;
  }

  public DataType getDataType() {
    return _dataType;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setDataType(DataType dataType) {
    _dataType = dataType;
    _defaultNullValue = getDefaultNullValue(getFieldType(), _dataType, _stringDefaultNullValue);
  }

  public boolean isSingleValueField() {
    return _isSingleValueField;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setSingleValueField(boolean isSingleValueField) {
    _isSingleValueField = isSingleValueField;
  }

  public int getMaxLength() {
    return _maxLength;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setMaxLength(int maxLength) {
    _maxLength = maxLength;
  }

  public String getVirtualColumnProvider() {
    return _virtualColumnProvider;
  }

  public void setVirtualColumnProvider(String virtualColumnProvider) {
    _virtualColumnProvider = virtualColumnProvider;
  }

  /**
   * Returns whether the column is virtual. Virtual columns are constructed while loading the segment, thus do not exist
   * in the record, nor should be persisted to the disk.
   * <p>Identify a column as virtual if the virtual column provider is configured.
   */
  @JsonIgnore
  public boolean isVirtualColumn() {
    return _virtualColumnProvider != null && !_virtualColumnProvider.isEmpty();
  }

  public Object getDefaultNullValue() {
    return _defaultNullValue;
  }

  public String getDefaultNullValueString() {
    return getStringValue(_defaultNullValue);
  }

  /**
   * Helper method to return the String value for the given object.
   * This is required as not all data types have a toString() (eg byte[]).
   *
   * @param value Value for which String value needs to be returned
   * @return String value for the object.
   */
  protected static String getStringValue(Object value) {
    if (value instanceof byte[]) {
      return BytesUtils.toHexString((byte[]) value);
    } else {
      return value.toString();
    }
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setDefaultNullValue(@Nullable Object defaultNullValue) {
    if (defaultNullValue != null) {
      _stringDefaultNullValue = getStringValue(defaultNullValue);
    }
    if (_dataType != null) {
      _defaultNullValue = getDefaultNullValue(getFieldType(), _dataType, _stringDefaultNullValue);
    }
  }

  private static Object getDefaultNullValue(FieldType fieldType, DataType dataType,
      @Nullable String stringDefaultNullValue) {
    if (stringDefaultNullValue != null) {
      return dataType.convert(stringDefaultNullValue);
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
            case BYTES:
              return DEFAULT_METRIC_NULL_VALUE_OF_BYTES;
            default:
              throw new IllegalStateException("Unsupported metric data type: " + dataType);
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
            case BOOLEAN:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_BOOLEAN;
            case TIMESTAMP:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP;
            case STRING:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
            case JSON:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_JSON;
            case BYTES:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES;
            default:
              throw new IllegalStateException("Unsupported dimension/time data type: " + dataType);
          }
        default:
          throw new IllegalStateException("Unsupported field type: " + fieldType);
      }
    }
  }

  /**
   * Transform function if defined else null.
   * Deprecated. Use TableConfig -> IngestionConfig -> TransformConfigs
   */
  @Deprecated
  public String getTransformFunction() {
    return _transformFunction;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.

  /**
   * Deprecated. Use TableConfig -> IngestionConfig -> TransformConfigs
   */
  @Deprecated
  public void setTransformFunction(@Nullable String transformFunction) {
    _transformFunction = transformFunction;
  }

  /**
   * Returns the {@link ObjectNode} representing the field spec.
   * <p>Only contains fields with non-default value.
   * <p>NOTE: here we use {@link ObjectNode} to preserve the insertion order.
   */
  public ObjectNode toJsonObject() {
    ObjectNode jsonObject = JsonUtils.newObjectNode();
    jsonObject.put("name", _name);
    jsonObject.put("dataType", _dataType.name());
    if (!_isSingleValueField) {
      jsonObject.put("singleValueField", false);
    }
    if (_maxLength != DEFAULT_MAX_LENGTH) {
      jsonObject.put("maxLength", _maxLength);
    }
    appendDefaultNullValue(jsonObject);
    appendTransformFunction(jsonObject);
    return jsonObject;
  }

  protected void appendDefaultNullValue(ObjectNode jsonNode) {
    assert _defaultNullValue != null;
    String key = "defaultNullValue";
    if (!_defaultNullValue.equals(getDefaultNullValue(getFieldType(), _dataType, null))) {
      switch (_dataType) {
        case INT:
          jsonNode.put(key, (Integer) _defaultNullValue);
          break;
        case LONG:
          jsonNode.put(key, (Long) _defaultNullValue);
          break;
        case FLOAT:
          jsonNode.put(key, (Float) _defaultNullValue);
          break;
        case DOUBLE:
          jsonNode.put(key, (Double) _defaultNullValue);
          break;
        case BOOLEAN:
          jsonNode.put(key, (Integer) _defaultNullValue == 1);
          break;
        case TIMESTAMP:
          jsonNode.put(key, new Timestamp((Long) _defaultNullValue).toString());
          break;
        case STRING:
        case JSON:
          jsonNode.put(key, (String) _defaultNullValue);
          break;
        case BYTES:
          jsonNode.put(key, BytesUtils.toHexString((byte[]) _defaultNullValue));
          break;
        default:
          throw new IllegalStateException("Unsupported data type: " + this);
      }
    }
  }

  protected void appendTransformFunction(ObjectNode jsonNode) {
    if (_transformFunction != null) {
      jsonNode.put("transformFunction", _transformFunction);
    }
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
        .isEqual(_isSingleValueField, that._isSingleValueField) && EqualityUtils
        .isEqual(getStringValue(_defaultNullValue), getStringValue(that._defaultNullValue)) && EqualityUtils
        .isEqual(_maxLength, that._maxLength) && EqualityUtils.isEqual(_transformFunction, that._transformFunction)
        && EqualityUtils.isEqual(_virtualColumnProvider, that._virtualColumnProvider);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_name);
    result = EqualityUtils.hashCodeOf(result, _dataType);
    result = EqualityUtils.hashCodeOf(result, _isSingleValueField);
    result = EqualityUtils.hashCodeOf(result, getStringValue(_defaultNullValue));
    result = EqualityUtils.hashCodeOf(result, _maxLength);
    result = EqualityUtils.hashCodeOf(result, _transformFunction);
    result = EqualityUtils.hashCodeOf(result, _virtualColumnProvider);
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
    DIMENSION, METRIC, TIME, DATE_TIME, COMPLEX
  }

  /**
   * The <code>DataType</code> enum is used to demonstrate the data type of a field.
   */
  @SuppressWarnings("rawtypes")
  public enum DataType {
    // LIST is for complex lists which is different from multi-value column of primitives
    // STRUCT, MAP and LIST are composable to form a COMPLEX field
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BOOLEAN /* Stored as INT */,
    TIMESTAMP /* Stored as LONG */,
    STRING,
    JSON /* Stored as STRING */,
    BYTES,
    STRUCT,
    MAP,
    LIST;

    /**
     * Returns the data type stored in Pinot.
     * <p>Pinot internally stores data (physical) in INT, LONG, FLOAT, DOUBLE, STRING, BYTES type, other data types
     * (logical) will be stored as one of these types.
     * <p>Stored type should be used when reading the physical stored values from Dictionary, Forward Index etc.
     */
    public DataType getStoredType() {
      switch (this) {
        case BOOLEAN:
          return INT;
        case TIMESTAMP:
          return LONG;
        case JSON:
          return STRING;
        default:
          return this;
      }
    }

    /**
     * Returns {@code true} if the data type is of fixed width (INT, LONG, FLOAT, DOUBLE, BOOLEAN, TIMESTAMP),
     * {@code false} otherwise.
     */
    public boolean isFixedWidth() {
      return this.ordinal() < STRING.ordinal();
    }

    /**
     * Returns the number of bytes needed to store the data type.
     */
    public int size() {
      switch (this) {
        case INT:
        case BOOLEAN:
          return Integer.BYTES;
        case LONG:
        case TIMESTAMP:
          return Long.BYTES;
        case FLOAT:
          return Float.BYTES;
        case DOUBLE:
          return Double.BYTES;
        default:
          throw new IllegalStateException("Cannot get number of bytes for: " + this);
      }
    }

    /**
     * Returns {@code true} if the data type is numeric (INT, LONG, FLOAT, DOUBLE), {@code false} otherwise.
     */
    public boolean isNumeric() {
      return this == INT || this == LONG || this == FLOAT || this == DOUBLE;
    }

    /**
     * Converts the given string value to the data type. Returns byte[] for BYTES.
     */
    public Object convert(String value) {
      try {
        switch (this) {
          case INT:
            return Integer.valueOf(value);
          case LONG:
            return Long.valueOf(value);
          case FLOAT:
            return Float.valueOf(value);
          case DOUBLE:
            return Double.valueOf(value);
          case BOOLEAN:
            return BooleanUtils.toInt(value);
          case TIMESTAMP:
            return TimestampUtils.toMillisSinceEpoch(value);
          case STRING:
          case JSON:
            return value;
          case BYTES:
            return BytesUtils.toBytes(value);
          default:
            throw new IllegalStateException();
        }
      } catch (Exception e) {
        throw new IllegalArgumentException(String.format("Cannot convert value: '%s' to type: %s", value, this));
      }
    }

    /**
     * Converts the given string value to the data type. Returns ByteArray for BYTES.
     */
    public Comparable convertInternal(String value) {
      try {
        switch (this) {
          case INT:
            return Integer.valueOf(value);
          case LONG:
            return Long.valueOf(value);
          case FLOAT:
            return Float.valueOf(value);
          case DOUBLE:
            return Double.valueOf(value);
          case BOOLEAN:
            return BooleanUtils.toInt(value);
          case TIMESTAMP:
            return TimestampUtils.toMillisSinceEpoch(value);
          case STRING:
          case JSON:
            return value;
          case BYTES:
            return BytesUtils.toByteArray(value);
          default:
            throw new IllegalStateException();
        }
      } catch (Exception e) {
        throw new IllegalArgumentException(String.format("Cannot convert value: '%s' to type: %s", value, this));
      }
    }

    /**
     * Checks whether the data type can be a sorted column.
     */
    public boolean canBeASortedColumn() {
      return this != BYTES && this != JSON && this != STRUCT && this != MAP && this != LIST;
    }
  }

  @Override
  public int compareTo(FieldSpec otherSpec) {
    // Sort fieldspecs based on their name
    return _name.compareTo(otherSpec._name);
  }
}
