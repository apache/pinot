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
package org.apache.pinot.common.data;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javax.annotation.Nullable;
import org.apache.avro.Schema.Type;
import org.apache.pinot.common.config.ConfigKey;
import org.apache.pinot.common.config.ConfigNodeLifecycleAware;
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.common.utils.EqualityUtils;
import org.apache.pinot.common.utils.JsonUtils;


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
public abstract class FieldSpec implements Comparable<FieldSpec>, ConfigNodeLifecycleAware {
  private static final int DEFAULT_MAX_LENGTH = 512;

  // TODO: revisit to see if we allow 0-length byte array
  private static final byte[] NULL_BYTE_ARRAY_VALUE = new byte[0];

  public static final Integer DEFAULT_DIMENSION_NULL_VALUE_OF_INT = Integer.MIN_VALUE;
  public static final Long DEFAULT_DIMENSION_NULL_VALUE_OF_LONG = Long.MIN_VALUE;
  public static final Float DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT = Float.NEGATIVE_INFINITY;
  public static final Double DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE = Double.NEGATIVE_INFINITY;
  public static final String DEFAULT_DIMENSION_NULL_VALUE_OF_STRING = "null";
  public static final byte[] DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES = NULL_BYTE_ARRAY_VALUE;

  public static final Integer DEFAULT_METRIC_NULL_VALUE_OF_INT = 0;
  public static final Long DEFAULT_METRIC_NULL_VALUE_OF_LONG = 0L;
  public static final Float DEFAULT_METRIC_NULL_VALUE_OF_FLOAT = 0.0F;
  public static final Double DEFAULT_METRIC_NULL_VALUE_OF_DOUBLE = 0.0D;
  public static final String DEFAULT_METRIC_NULL_VALUE_OF_STRING = "null";
  public static final byte[] DEFAULT_METRIC_NULL_VALUE_OF_BYTES = NULL_BYTE_ARRAY_VALUE;

  @ConfigKey("name")
  protected String _name;

  @ConfigKey("dataType")
  protected DataType _dataType;

  @ConfigKey("singleValue")
  protected boolean _isSingleValueField = true;

  // NOTE: for STRING column, this is the max number of characters; for BYTES column, this is the max number of bytes
  @ConfigKey("maxLength")
  private int _maxLength = DEFAULT_MAX_LENGTH;

  protected Object _defaultNullValue;

  @ConfigKey("defaultNullValue")
  private transient String _stringDefaultNullValue;

  // Transform function to generate this column, can be based on other columns
  protected String _transformFunction;

  @ConfigKey("virtualColumnProvider")
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
    _dataType = dataType.getStoredType();
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

  public Object getDefaultNullValue() {
    return _defaultNullValue;
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
            case BYTES:
              return DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES;
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
   * Transform function if defined else null.
   * @return
   */
  public String getTransformFunction() {
    return _transformFunction;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setTransformFunction(String transformFunction) {
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
    return jsonObject;
  }

  protected void appendDefaultNullValue(ObjectNode jsonNode) {
    assert _defaultNullValue != null;
    if (!_defaultNullValue.equals(getDefaultNullValue(getFieldType(), _dataType, null))) {
      if (_defaultNullValue instanceof Number) {
        jsonNode.set("defaultNullValue", JsonUtils.objectToJsonNode(_defaultNullValue));
      } else {
        jsonNode.put("defaultNullValue", getStringValue(_defaultNullValue));
      }
    }
  }

  public ObjectNode toAvroSchemaJsonObject() {
    ObjectNode jsonSchema = JsonUtils.newObjectNode();
    jsonSchema.put("name", _name);
    switch (_dataType) {
      case INT:
        jsonSchema.set("type", convertStringsToJsonArray("null", "int"));
        return jsonSchema;
      case LONG:
        jsonSchema.set("type", convertStringsToJsonArray("null", "long"));
        return jsonSchema;
      case FLOAT:
        jsonSchema.set("type", convertStringsToJsonArray("null", "float"));
        return jsonSchema;
      case DOUBLE:
        jsonSchema.set("type", convertStringsToJsonArray("null", "double"));
        return jsonSchema;
      case STRING:
        jsonSchema.set("type", convertStringsToJsonArray("null", "string"));
        return jsonSchema;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private static ArrayNode convertStringsToJsonArray(String... strings) {
    ArrayNode jsonArray = JsonUtils.newArrayNode();
    for (String string : strings) {
      jsonArray.add(string);
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
        .isEqual(_isSingleValueField, that._isSingleValueField) && EqualityUtils
        .isEqual(getStringValue(_defaultNullValue), getStringValue(that._defaultNullValue)) && EqualityUtils
        .isEqual(_maxLength, that._maxLength);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_name);
    result = EqualityUtils.hashCodeOf(result, _dataType);
    result = EqualityUtils.hashCodeOf(result, _isSingleValueField);
    result = EqualityUtils.hashCodeOf(result, getStringValue(_defaultNullValue));
    result = EqualityUtils.hashCodeOf(result, _maxLength);
    return result;
  }

  @Override
  public void preInject() {
    // Nothing to do
  }

  @Override
  public void postInject() {
    // Compute the actual default null value from its string representation
    _defaultNullValue = getDefaultNullValue(getFieldType(), _dataType, _stringDefaultNullValue);
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
    DIMENSION, METRIC, TIME, DATE_TIME
  }

  /**
   * The <code>DataType</code> enum is used to demonstrate the data type of a field.
   */
  public enum DataType {
    INT, LONG, FLOAT, DOUBLE, BOOLEAN/* Stored as STRING */, STRING, BYTES;

    /**
     * Returns the data type stored in Pinot.
     */
    public DataType getStoredType() {
      return this == BOOLEAN ? STRING : this;
    }

    /**
     * Returns the data type stored in Pinot that is associated with the given Avro type.
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
        case BYTES:
          return BYTES;
        default:
          throw new UnsupportedOperationException("Unsupported Avro type: " + avroType);
      }
    }

    /**
     * Returns the number of bytes needed to store the data type.
     */
    public int size() {
      switch (this) {
        case INT:
          return Integer.BYTES;
        case LONG:
          return Long.BYTES;
        case FLOAT:
          return Float.BYTES;
        case DOUBLE:
          return Double.BYTES;
        case BYTES:
          // TODO: Metric size is only used for Star-tree generation, which is not supported yet.
          return MetricFieldSpec.UNDEFINED_METRIC_SIZE;
        default:
          throw new IllegalStateException("Cannot get number of bytes for: " + this);
      }
    }

    /**
     * Converts the given string value to the data type.
     */
    public Object convert(String value) {
      switch (this) {
        case INT:
          return Integer.valueOf(value);
        case LONG:
          return Long.valueOf(value);
        case FLOAT:
          return Float.valueOf(value);
        case DOUBLE:
          return Double.valueOf(value);
        case STRING:
          return value;
        case BYTES:
          return BytesUtils.toBytes(value);
        default:
          throw new UnsupportedOperationException("Unsupported data type: " + this);
      }
    }
  }

  @Override
  public int compareTo(FieldSpec otherSpec) {
    // Sort fieldspecs based on their name
    return _name.compareTo(otherSpec._name);
  }
}
