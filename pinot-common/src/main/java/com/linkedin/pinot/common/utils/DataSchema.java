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
package com.linkedin.pinot.common.utils;

import com.linkedin.pinot.common.data.FieldSpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import javax.annotation.Nonnull;


/**
 * The <code>DataSchema</code> class describes the schema of {@link DataTable}.
 */
public class DataSchema {
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private String[] _columnNames;
  private FieldSpec.DataType[] _columnTypes;

  public DataSchema(@Nonnull String[] columnNames, @Nonnull FieldSpec.DataType[] columnTypes) {
    _columnNames = columnNames;
    _columnTypes = columnTypes;
  }

  public int size() {
    return _columnNames.length;
  }

  @Nonnull
  public String getColumnName(int index) {
    return _columnNames[index];
  }

  @Nonnull
  public FieldSpec.DataType getColumnType(int index) {
    return _columnTypes[index];
  }

  /**
   * Indicates whether the given {@link DataSchema} is type compatible with this one.
   * <ul>
   *   <li>All numbers are type compatible.</li>
   *   <li>Number is not type compatible with String.</li>
   *   <li>Single-value is not type compatible with multi-value.</li>
   * </ul>
   *
   * @param anotherDataSchema data schema to compare.
   * @return whether the two data schemas are type compatible.
   */
  public boolean isTypeCompatibleWith(@Nonnull DataSchema anotherDataSchema) {
    if (!Arrays.equals(_columnNames, anotherDataSchema._columnNames)) {
      return false;
    }
    int numColumns = _columnNames.length;
    for (int i = 0; i < numColumns; i++) {
      if (!_columnTypes[i].isCompatible(anotherDataSchema._columnTypes[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Upgrade the current data schema to cover the column types in the given data schema.
   * <p>Type <code>long</code> can cover <code>int</code> and <code>long</code>.
   * <p>Type <code>double</code> can cover all numbers, but with potential precision loss when use it to cover
   * <code>long</code>.
   * <p>The given data schema should be type compatible with this one.
   *
   * @param anotherDataSchema data schema to be covered.
   */
  public void upgradeToCover(@Nonnull DataSchema anotherDataSchema) {
    int numColumns = _columnTypes.length;
    for (int i = 0; i < numColumns; i++) {
      FieldSpec.DataType thisColumnType = _columnTypes[i];
      FieldSpec.DataType thatColumnType = anotherDataSchema._columnTypes[i];
      if (thisColumnType != thatColumnType) {
        if (thisColumnType.isSingleValue()) {
          if (thisColumnType.isInteger() && thatColumnType.isInteger()) {
            _columnTypes[i] = FieldSpec.DataType.LONG;
          } else {
            _columnTypes[i] = FieldSpec.DataType.DOUBLE;
          }
        } else {
          if (thisColumnType.toSingleValue().isInteger() && thatColumnType.toSingleValue().isInteger()) {
            _columnTypes[i] = FieldSpec.DataType.LONG_ARRAY;
          } else {
            _columnTypes[i] = FieldSpec.DataType.DOUBLE_ARRAY;
          }
        }
      }
    }
  }

  @Nonnull
  public byte[] toBytes()
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    int length = _columnNames.length;

    // Write the number of columns.
    dataOutputStream.writeInt(length);

    // Write the column names.
    for (String columnName : _columnNames) {
      byte[] bytes = columnName.getBytes(UTF_8);
      dataOutputStream.writeInt(bytes.length);
      dataOutputStream.write(bytes);
    }

    // Write the column types.
    for (FieldSpec.DataType columnType : _columnTypes) {
      // We don't want to use ordinal of the enum since adding a new data type will break things if server and broker
      // use different versions of DataType class.
      byte[] bytes = columnType.name().getBytes(UTF_8);
      dataOutputStream.writeInt(bytes.length);
      dataOutputStream.write(bytes);
    }
    return byteArrayOutputStream.toByteArray();
  }

  @Nonnull
  public static DataSchema fromBytes(@Nonnull byte[] buffer)
      throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer);
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);

    // Read the number of columns.
    int numColumns = dataInputStream.readInt();
    String[] columnNames = new String[numColumns];
    FieldSpec.DataType[] columnTypes = new FieldSpec.DataType[numColumns];

    // Read the column names.
    int readLength;
    for (int i = 0; i < numColumns; i++) {
      int length = dataInputStream.readInt();
      byte[] bytes = new byte[length];
      readLength = dataInputStream.read(bytes);
      assert readLength == length;
      columnNames[i] = new String(bytes, UTF_8);
    }

    // Read the column types.
    for (int i = 0; i < numColumns; i++) {
      int length = dataInputStream.readInt();
      byte[] bytes = new byte[length];
      readLength = dataInputStream.read(bytes);
      assert readLength == length;
      columnTypes[i] = FieldSpec.DataType.valueOf(new String(bytes, UTF_8));
    }

    return new DataSchema(columnNames, columnTypes);
  }

  @SuppressWarnings("CloneDoesntCallSuperClone")
  @Override
  public DataSchema clone() {
    return new DataSchema(_columnNames.clone(), _columnTypes.clone());
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append('[');
    int numColumns = _columnNames.length;
    for (int i = 0; i < numColumns; i++) {
      stringBuilder.append(_columnNames[i]).append('(').append(_columnTypes[i]).append(')').append(',');
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
      return Arrays.equals(_columnNames, anotherDataSchema._columnNames) && Arrays.equals(_columnTypes,
          anotherDataSchema._columnTypes);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = EqualityUtils.hashCodeOf(_columnNames);
    hashCode = EqualityUtils.hashCodeOf(hashCode, _columnTypes);
    return hashCode;
  }
}
