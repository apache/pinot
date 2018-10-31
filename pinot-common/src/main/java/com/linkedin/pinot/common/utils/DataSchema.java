/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.utils;

import com.linkedin.pinot.common.data.FieldSpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import javax.annotation.Nonnull;


/**
 * The <code>DataSchema</code> class describes the schema of {@link DataTable}.
 */
public class DataSchema {
  private String[] _columnNames;
  private ColumnDataType[] _columnDataTypes;

  public DataSchema(@Nonnull String[] columnNames, @Nonnull ColumnDataType[] columnDataTypes) {
    _columnNames = columnNames;
    _columnDataTypes = columnDataTypes;
  }

  public int size() {
    return _columnNames.length;
  }

  @Nonnull
  public String getColumnName(int index) {
    return _columnNames[index];
  }

  @Nonnull
  public ColumnDataType getColumnDataType(int index) {
    return _columnDataTypes[index];
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
  public boolean isTypeCompatibleWith(@Nonnull DataSchema anotherDataSchema) {
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
  public void upgradeToCover(@Nonnull DataSchema anotherDataSchema) {
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

  @Nonnull
  public byte[] toBytes() throws IOException {
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

  @Nonnull
  public static DataSchema fromBytes(@Nonnull byte[] buffer) throws IOException {
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
      return Arrays.equals(_columnNames, anotherDataSchema._columnNames) && Arrays.equals(_columnDataTypes,
          anotherDataSchema._columnDataTypes);
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
      switch (dataType) {
        case INT:
          return isSingleValue ? INT : INT_ARRAY;
        case LONG:
          return isSingleValue ? LONG : LONG_ARRAY;
        case FLOAT:
          return isSingleValue ? FLOAT : FLOAT_ARRAY;
        case DOUBLE:
          return isSingleValue ? DOUBLE : DOUBLE_ARRAY;
        case STRING:
          return isSingleValue ? STRING : STRING_ARRAY;
        case BYTES:
          if (isSingleValue) {
            return BYTES;
          } // else, fall down to default.
        default:
          throw new UnsupportedOperationException("Unsupported data type: " + dataType);
      }
    }
  }
}
