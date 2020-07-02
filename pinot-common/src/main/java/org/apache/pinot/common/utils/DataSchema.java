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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.EqualityUtils;


/**
 * The <code>DataSchema</code> class describes the schema of {@link DataTable}.
 */
public class DataSchema {
  private final String[] _columnNames;
  private final ColumnDataType[] _columnDataTypes;

  public DataSchema(String[] columnNames, ColumnDataType[] columnDataTypes) {
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

  @SuppressWarnings("CloneDoesntCallSuperClone")
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
    INT, LONG, FLOAT, DOUBLE, STRING, BYTES, OBJECT, INT_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY, STRING_ARRAY;

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

    public static ColumnDataType fromDataType(FieldSpec.DataType dataType, boolean isSingleValue) {
      return isSingleValue ? fromDataTypeSV(dataType) : fromDataTypeMV(dataType);
    }

    public static ColumnDataType fromDataTypeSV(FieldSpec.DataType dataType) {
      switch (dataType) {
        case INT:
          return INT;
        case LONG:
          return LONG;
        case FLOAT:
          return FLOAT;
        case DOUBLE:
          return DOUBLE;
        case STRING:
          return STRING;
        case BYTES:
          return BYTES;
        default:
          throw new IllegalStateException("Unsupported data type: " + dataType);
      }
    }

    public static ColumnDataType fromDataTypeMV(FieldSpec.DataType dataType) {
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
