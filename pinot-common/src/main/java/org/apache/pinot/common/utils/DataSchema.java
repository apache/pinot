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
package org.apache.pinot.common.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.EqualityUtils;


/**
 * The <code>DataSchema</code> class describes the schema of {@link DataTable}.
 */
@JsonPropertyOrder({"columnNames", "columnDataTypes"})
public class DataSchema {
  private final String[] _columnNames;
  private final ColumnDataType[] _columnDataTypes;
  private ColumnDataType[] _storedColumnDataTypes;

  @JsonCreator
  public DataSchema(@JsonProperty("columnNames") String[] columnNames,
      @JsonProperty("columnDataTypes") ColumnDataType[] columnDataTypes) {
    _columnNames = columnNames;
    _columnDataTypes = columnDataTypes;
  }

  public int size() {
    return _columnNames.length;
  }

  public String getColumnName(int index) {
    return _columnNames[index];
  }

  public String[] getColumnNames() {
    return _columnNames;
  }

  public ColumnDataType getColumnDataType(int index) {
    return _columnDataTypes[index];
  }

  public ColumnDataType[] getColumnDataTypes() {
    return _columnDataTypes;
  }

  @JsonIgnore
  public ColumnDataType[] getStoredColumnDataTypes() {
    if (_storedColumnDataTypes == null) {
      int numColumns = _columnDataTypes.length;
      _storedColumnDataTypes = new ColumnDataType[numColumns];
      for (int i = 0; i < numColumns; i++) {
        _storedColumnDataTypes[i] = _columnDataTypes[i].getStoredType();
      }
    }
    return _storedColumnDataTypes;
  }

  /**
   * Returns whether the given data schema is type compatible with this one.
   * <ul>
   *   <li>All numbers are type compatible with each other</li>
   *   <li>Numbers are not type compatible with string</li>
   *   <li>Non-array types are not type compatible with array types</li>
   * </ul>
   *
   * @param anotherDataSchema Data schema to compare with
   * @return Whether the two data schemas are type compatible
   */
  public boolean isTypeCompatibleWith(DataSchema anotherDataSchema) {
    if (!Arrays.equals(_columnNames, anotherDataSchema._columnNames)) {
      return false;
    }
    int numColumns = _columnNames.length;
    for (int i = 0; i < numColumns; i++) {
      if (!_columnDataTypes[i].isCompatible(anotherDataSchema._columnDataTypes[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Upgrade the current data schema to cover the column data types in the given data schema.
   * <p>Data type <code>LONG</code> can cover <code>INT</code> and <code>LONG</code>.
   * <p>Data type <code>DOUBLE</code> can cover all numbers, but with potential precision loss when use it to cover
   * <code>LONG</code>.
   * <p>NOTE: The given data schema should be type compatible with this one.
   *
   * @param anotherDataSchema Data schema to cover
   */
  public void upgradeToCover(DataSchema anotherDataSchema) {
    int numColumns = _columnDataTypes.length;
    for (int i = 0; i < numColumns; i++) {
      ColumnDataType thisColumnDataType = _columnDataTypes[i];
      ColumnDataType thatColumnDataType = anotherDataSchema._columnDataTypes[i];
      if (thisColumnDataType != thatColumnDataType) {
        if (thisColumnDataType.isArray()) {
          if (thisColumnDataType.isWholeNumberArray() && thatColumnDataType.isWholeNumberArray()) {
            _columnDataTypes[i] = ColumnDataType.LONG_ARRAY;
          } else {
            _columnDataTypes[i] = ColumnDataType.DOUBLE_ARRAY;
          }
        } else {
          if (thisColumnDataType.isWholeNumber() && thatColumnDataType.isWholeNumber()) {
            _columnDataTypes[i] = ColumnDataType.LONG;
          } else {
            _columnDataTypes[i] = ColumnDataType.DOUBLE;
          }
        }
      }
    }
  }

  public byte[] toBytes()
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    int length = _columnNames.length;

    // Write the number of columns.
    dataOutputStream.writeInt(length);

    // Write the column names.
    for (String columnName : _columnNames) {
      byte[] bytes = StringUtil.encodeUtf8(columnName);
      dataOutputStream.writeInt(bytes.length);
      dataOutputStream.write(bytes);
    }

    // Write the column types.
    for (ColumnDataType columnDataType : _columnDataTypes) {
      // We don't want to use ordinal of the enum since adding a new data type will break things if server and broker
      // use different versions of DataType class.
      byte[] bytes = StringUtil.encodeUtf8(columnDataType.name());
      dataOutputStream.writeInt(bytes.length);
      dataOutputStream.write(bytes);
    }
    return byteArrayOutputStream.toByteArray();
  }

  public static DataSchema fromBytes(byte[] buffer)
      throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer);
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);

    // Read the number of columns.
    int numColumns = dataInputStream.readInt();
    String[] columnNames = new String[numColumns];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numColumns];

    // Read the column names.
    int readLength;
    for (int i = 0; i < numColumns; i++) {
      int length = dataInputStream.readInt();
      byte[] bytes = new byte[length];
      readLength = dataInputStream.read(bytes);
      assert readLength == length;
      columnNames[i] = StringUtil.decodeUtf8(bytes);
    }

    // Read the column types.
    for (int i = 0; i < numColumns; i++) {
      int length = dataInputStream.readInt();
      byte[] bytes = new byte[length];
      readLength = dataInputStream.read(bytes);
      assert readLength == length;
      columnDataTypes[i] = ColumnDataType.valueOf(StringUtil.decodeUtf8(bytes));
    }

    return new DataSchema(columnNames, columnDataTypes);
  }

  @SuppressWarnings("MethodDoesntCallSuperMethod")
  @Override
  public DataSchema clone() {
    return new DataSchema(_columnNames.clone(), _columnDataTypes.clone());
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append('[');
    int numColumns = _columnNames.length;
    for (int i = 0; i < numColumns; i++) {
      stringBuilder.append(_columnNames[i]).append('(').append(_columnDataTypes[i]).append(')').append(',');
    }
    stringBuilder.setCharAt(stringBuilder.length() - 1, ']');
    return stringBuilder.toString();
  }

  @Override
  public boolean equals(Object anObject) {
    if (this == anObject) {
      return true;
    }
    if (anObject instanceof DataSchema) {
      DataSchema anotherDataSchema = (DataSchema) anObject;
      return Arrays.equals(_columnNames, anotherDataSchema._columnNames) && Arrays
          .equals(_columnDataTypes, anotherDataSchema._columnDataTypes);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return EqualityUtils.hashCodeOf(Arrays.hashCode(_columnNames), Arrays.hashCode(_columnDataTypes));
  }

  public enum ColumnDataType {
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BOOLEAN /* Stored as INT */,
    TIMESTAMP /* Stored as LONG */,
    STRING,
    JSON /* Stored as STRING */,
    BYTES,
    OBJECT,
    INT_ARRAY,
    LONG_ARRAY,
    FLOAT_ARRAY,
    DOUBLE_ARRAY,
    STRING_ARRAY;

    /**
     * Returns the data type stored in Pinot.
     */
    public ColumnDataType getStoredType() {
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

    public boolean isNumber() {
      return this == INT || this == LONG || this == FLOAT || this == DOUBLE;
    }

    public boolean isWholeNumber() {
      return this == INT || this == LONG;
    }

    public boolean isArray() {
      return this == INT_ARRAY || this == LONG_ARRAY || this == FLOAT_ARRAY || this == DOUBLE_ARRAY
          || this == STRING_ARRAY;
    }

    public boolean isNumberArray() {
      return this == INT_ARRAY || this == LONG_ARRAY || this == FLOAT_ARRAY || this == DOUBLE_ARRAY;
    }

    public boolean isWholeNumberArray() {
      return this == INT_ARRAY || this == LONG_ARRAY;
    }

    public boolean isCompatible(ColumnDataType anotherColumnDataType) {
      // All numbers are compatible with each other
      return this == anotherColumnDataType || (this.isNumber() && anotherColumnDataType.isNumber()) || (
          this.isNumberArray() && anotherColumnDataType.isNumberArray());
    }

    public DataType toDataType() {
      switch (this) {
        case INT:
          return DataType.INT;
        case LONG:
          return DataType.LONG;
        case FLOAT:
          return DataType.FLOAT;
        case DOUBLE:
          return DataType.DOUBLE;
        case BOOLEAN:
          return DataType.BOOLEAN;
        case TIMESTAMP:
          return DataType.TIMESTAMP;
        case STRING:
          return DataType.STRING;
        case JSON:
          return DataType.JSON;
        case BYTES:
          return DataType.BYTES;
        default:
          throw new IllegalStateException(String.format("Cannot convert ColumnDataType: %s to DataType", this));
      }
    }

    /**
     * Converts the given internal value to the type for external use (e.g. as UDF argument). The given value should be
     * compatible with the type.
     */
    public Serializable convert(Object value) {
      switch (this) {
        case INT:
          return ((Number) value).intValue();
        case LONG:
          return ((Number) value).longValue();
        case FLOAT:
          return ((Number) value).floatValue();
        case DOUBLE:
          return ((Number) value).doubleValue();
        case BOOLEAN:
          return (Integer) value == 1;
        case TIMESTAMP:
          return new Timestamp((Long) value);
        case STRING:
        case JSON:
          return value.toString();
        case BYTES:
          return ((ByteArray) value).getBytes();
        case INT_ARRAY:
          return (int[]) value;
        case LONG_ARRAY:
          if (value instanceof long[]) {
            return (long[]) value;
          } else {
            int[] intValues = (int[]) value;
            int length = intValues.length;
            long[] longValues = new long[length];
            for (int i = 0; i < length; i++) {
              longValues[i] = intValues[i];
            }
            return longValues;
          }
        case FLOAT_ARRAY:
          return (float[]) value;
        case DOUBLE_ARRAY:
          if (value instanceof double[]) {
            return (double[]) value;
          } else if (value instanceof int[]) {
            int[] intValues = (int[]) value;
            int length = intValues.length;
            double[] doubleValues = new double[length];
            for (int i = 0; i < length; i++) {
              doubleValues[i] = intValues[i];
            }
            return doubleValues;
          } else if (value instanceof long[]) {
            long[] longValues = (long[]) value;
            int length = longValues.length;
            double[] doubleValues = new double[length];
            for (int i = 0; i < length; i++) {
              doubleValues[i] = longValues[i];
            }
            return doubleValues;
          } else {
            float[] floatValues = (float[]) value;
            int length = floatValues.length;
            double[] doubleValues = new double[length];
            for (int i = 0; i < length; i++) {
              doubleValues[i] = floatValues[i];
            }
            return doubleValues;
          }
        case STRING_ARRAY:
          return (String[]) value;
        default:
          throw new IllegalStateException(String.format("Cannot convert: '%s' to type: %s", value, this));
      }
    }

    /**
     * Formats the value to human-readable format based on the type to be used in the query response.
     */
    public Serializable format(Object value) {
      switch (this) {
        case TIMESTAMP:
          assert value instanceof Timestamp;
          return value.toString();
        case BYTES:
          return BytesUtils.toHexString((byte[]) value);
        default:
          return (Serializable) value;
      }
    }

    /**
     * Equivalent to {@link #convert(Object)} and {@link #format(Object)} with a single switch statement.
     */
    public Serializable convertAndFormat(Object value) {
      switch (this) {
        case INT:
          return ((Number) value).intValue();
        case LONG:
          return ((Number) value).longValue();
        case FLOAT:
          return ((Number) value).floatValue();
        case DOUBLE:
          return ((Number) value).doubleValue();
        case BOOLEAN:
          return (Integer) value == 1;
        case TIMESTAMP:
          return new Timestamp((Long) value).toString();
        case STRING:
        case JSON:
          return value.toString();
        case BYTES:
          return ((ByteArray) value).toHexString();
        case INT_ARRAY:
          return (int[]) value;
        case LONG_ARRAY:
          if (value instanceof long[]) {
            return (long[]) value;
          } else {
            int[] intValues = (int[]) value;
            int length = intValues.length;
            long[] longValues = new long[length];
            for (int i = 0; i < length; i++) {
              longValues[i] = intValues[i];
            }
            return longValues;
          }
        case FLOAT_ARRAY:
          return (float[]) value;
        case DOUBLE_ARRAY:
          if (value instanceof double[]) {
            return (double[]) value;
          } else if (value instanceof int[]) {
            int[] intValues = (int[]) value;
            int length = intValues.length;
            double[] doubleValues = new double[length];
            for (int i = 0; i < length; i++) {
              doubleValues[i] = intValues[i];
            }
            return doubleValues;
          } else if (value instanceof long[]) {
            long[] longValues = (long[]) value;
            int length = longValues.length;
            double[] doubleValues = new double[length];
            for (int i = 0; i < length; i++) {
              doubleValues[i] = longValues[i];
            }
            return doubleValues;
          } else {
            float[] floatValues = (float[]) value;
            int length = floatValues.length;
            double[] doubleValues = new double[length];
            for (int i = 0; i < length; i++) {
              doubleValues[i] = floatValues[i];
            }
            return doubleValues;
          }
        case STRING_ARRAY:
          return (String[]) value;
        default:
          throw new IllegalStateException(String.format("Cannot convert and format: '%s' to type: %s", value, this));
      }
    }

    public static ColumnDataType fromDataType(DataType dataType, boolean isSingleValue) {
      return isSingleValue ? fromDataTypeSV(dataType) : fromDataTypeMV(dataType);
    }

    public static ColumnDataType fromDataTypeSV(DataType dataType) {
      switch (dataType) {
        case INT:
          return INT;
        case LONG:
          return LONG;
        case FLOAT:
          return FLOAT;
        case DOUBLE:
          return DOUBLE;
        case BOOLEAN:
          return BOOLEAN;
        case TIMESTAMP:
          return TIMESTAMP;
        case STRING:
          return STRING;
        case JSON:
          return JSON;
        case BYTES:
          return BYTES;
        default:
          throw new IllegalStateException("Unsupported data type: " + dataType);
      }
    }

    public static ColumnDataType fromDataTypeMV(DataType dataType) {
      switch (dataType) {
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
          throw new IllegalStateException("Unsupported data type: " + dataType);
      }
    }
  }
}
