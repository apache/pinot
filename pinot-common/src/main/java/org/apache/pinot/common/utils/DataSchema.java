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
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.EnumSet;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.EqualityUtils;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * The <code>DataSchema</code> class describes the schema of {@link DataTable}.
 */
@JsonPropertyOrder({"columnNames", "columnDataTypes"})
public class DataSchema {
  private final String[] _columnNames;
  private final ColumnDataType[] _columnDataTypes;
  private ColumnDataType[] _storedColumnDataTypes;

  /** Used by both Broker and Server to generate results for EXPLAIN PLAN queries. */
  public static final DataSchema EXPLAIN_RESULT_SCHEMA =
      new DataSchema(new String[]{"Operator", "Operator_Id", "Parent_Id"}, new ColumnDataType[]{
          ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.INT
      });

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

  /**
   * Lazy compute the _storeColumnDataTypes field.
   */
  @JsonIgnore
  public ColumnDataType[] getStoredColumnDataTypes() {
    ColumnDataType[] storedColumnDataTypes = _storedColumnDataTypes;
    if (storedColumnDataTypes == null) {
      int numColumns = _columnDataTypes.length;
      storedColumnDataTypes = new ColumnDataType[numColumns];
      for (int i = 0; i < numColumns; i++) {
        storedColumnDataTypes[i] = _columnDataTypes[i].getStoredType();
      }
      _storedColumnDataTypes = storedColumnDataTypes;
    }
    return storedColumnDataTypes;
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
      byte[] bytes = columnName.getBytes(UTF_8);
      dataOutputStream.writeInt(bytes.length);
      dataOutputStream.write(bytes);
    }

    // Write the column types.
    for (ColumnDataType columnDataType : _columnDataTypes) {
      // We don't want to use ordinal of the enum since adding a new data type will break things if server and broker
      // use different versions of DataType class.
      byte[] bytes = columnDataType.name().getBytes(UTF_8);
      dataOutputStream.writeInt(bytes.length);
      dataOutputStream.write(bytes);
    }
    return byteArrayOutputStream.toByteArray();
  }

  /**
   * This method use relative operations on the ByteBuffer and expects the buffer's position to be set correctly.
   */
  public static DataSchema fromBytes(ByteBuffer buffer)
      throws IOException {
    // Read the number of columns.
    int numColumns = buffer.getInt();
    String[] columnNames = new String[numColumns];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numColumns];
    // Read the column names.
    for (int i = 0; i < numColumns; i++) {
      int length = buffer.getInt();
      byte[] bytes = new byte[length];
      buffer.get(bytes);
      columnNames[i] = new String(bytes, UTF_8);
    }
    // Read the column types.
    for (int i = 0; i < numColumns; i++) {
      int length = buffer.getInt();
      byte[] bytes = new byte[length];
      buffer.get(bytes);
      columnDataTypes[i] = ColumnDataType.valueOf(new String(bytes, UTF_8));
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
    INT(0),
    LONG(0L),
    FLOAT(0f),
    DOUBLE(0d),
    BIG_DECIMAL(BigDecimal.ZERO),
    BOOLEAN(INT, 0),
    TIMESTAMP(LONG, 0L),
    STRING(""),
    JSON(STRING, ""),
    BYTES(new ByteArray(new byte[0])),
    OBJECT(null),
    INT_ARRAY(new int[0]),
    LONG_ARRAY(new long[0]),
    FLOAT_ARRAY(new float[0]),
    DOUBLE_ARRAY(new double[0]),
    BOOLEAN_ARRAY(INT_ARRAY, new int[0]),
    TIMESTAMP_ARRAY(LONG_ARRAY, new long[0]),
    STRING_ARRAY(new String[0]),
    BYTES_ARRAY(new byte[0][]),
    UNKNOWN(null);

    private static final EnumSet<ColumnDataType> NUMERIC_TYPES = EnumSet.of(INT, LONG, FLOAT, DOUBLE, BIG_DECIMAL);
    private static final EnumSet<ColumnDataType> INTEGRAL_TYPES = EnumSet.of(INT, LONG);
    private static final EnumSet<ColumnDataType> ARRAY_TYPES =
        EnumSet.of(INT_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY, STRING_ARRAY, BOOLEAN_ARRAY, TIMESTAMP_ARRAY,
            BYTES_ARRAY);
    private static final EnumSet<ColumnDataType> NUMERIC_ARRAY_TYPES =
        EnumSet.of(INT_ARRAY, LONG_ARRAY, FLOAT_ARRAY, DOUBLE_ARRAY);
    private static final EnumSet<ColumnDataType> INTEGRAL_ARRAY_TYPES = EnumSet.of(INT_ARRAY, LONG_ARRAY);

    // stored data type.
    private final ColumnDataType _storedColumnDataType;

    // Placeholder for null. We need a placeholder for null so that it can be serialized in the data table
    private final Object _nullPlaceholder;

    ColumnDataType(Object nullPlaceHolder) {
      _storedColumnDataType = this;
      _nullPlaceholder = nullPlaceHolder;
    }

    ColumnDataType(ColumnDataType storedColumnDataType, Object nullPlaceHolder) {
      _storedColumnDataType = storedColumnDataType;
      _nullPlaceholder = nullPlaceHolder;
    }

    public Object getNullPlaceholder() {
      return _nullPlaceholder;
    }

    /**
     * Returns the data type stored in Pinot.
     */
    public ColumnDataType getStoredType() {
      return _storedColumnDataType;
    }

    public boolean isNumber() {
      return NUMERIC_TYPES.contains(this);
    }

    public boolean isWholeNumber() {
      return INTEGRAL_TYPES.contains(this);
    }

    public boolean isArray() {
      return ARRAY_TYPES.contains(this);
    }

    public boolean isNumberArray() {
      return NUMERIC_ARRAY_TYPES.contains(this);
    }

    public boolean isWholeNumberArray() {
      return INTEGRAL_ARRAY_TYPES.contains(this);
    }

    public boolean isCompatible(ColumnDataType anotherColumnDataType) {
      // All numbers are compatible with each other
      return this == anotherColumnDataType || (this.isNumber() && anotherColumnDataType.isNumber()) || (
          this.isNumberArray() && anotherColumnDataType.isNumberArray());
    }

    public DataType toDataType() {
      switch (this) {
        case INT:
        case INT_ARRAY:
          return DataType.INT;
        case LONG:
        case LONG_ARRAY:
          return DataType.LONG;
        case FLOAT:
        case FLOAT_ARRAY:
          return DataType.FLOAT;
        case DOUBLE:
        case DOUBLE_ARRAY:
          return DataType.DOUBLE;
        case BIG_DECIMAL:
          return DataType.BIG_DECIMAL;
        case BOOLEAN:
        case BOOLEAN_ARRAY:
          return DataType.BOOLEAN;
        case TIMESTAMP:
        case TIMESTAMP_ARRAY:
          return DataType.TIMESTAMP;
        case STRING:
        case STRING_ARRAY:
          return DataType.STRING;
        case JSON:
          return DataType.JSON;
        case BYTES:
        case BYTES_ARRAY:
          return DataType.BYTES;
        case UNKNOWN:
          return DataType.UNKNOWN;
        default:
          throw new IllegalStateException(String.format("Cannot convert ColumnDataType: %s to DataType", this));
      }
    }

    /**
     * Converts the value from external value type to the internal value type.
     *
     * <p>External value type is used in the following places:
     * <ul>
     *   <li>Data ingestion</li>
     *   <li>Scalar function arguments (UDF)</li>
     *   <li>Query response</li>
     * </ul>
     *
     * <p>Internal value type is used within the storage and query engine, where value is always of the stored type. For
     * BYTES type, we use a wrapper class {@link ByteArray} to make it comparable.
     *
     * <p>The conversion applies to the following types:
     * <ul>
     *   <li>BOOLEAN: boolean -> int</li>
     *   <li>TIMESTAMP: Timestamp -> long</li>
     *   <li>BYTES: byte[] -> ByteArray</li>
     *   <li>BOOLEAN_ARRAY: boolean[] -> int[]</li>
     *   <li>TIMESTAMP_ARRAY: Timestamp[] -> long[]</li>
     * </ul>
     */
    public Object toInternal(Object value) {
      switch (this) {
        case BOOLEAN:
          return ((boolean) value) ? 1 : 0;
        case TIMESTAMP:
          return ((Timestamp) value).getTime();
        case BYTES:
          return new ByteArray((byte[]) value);
        case BOOLEAN_ARRAY:
          return fromBooleanArray((boolean[]) value);
        case TIMESTAMP_ARRAY:
          return fromTimestampArray((Timestamp[]) value);
        case OBJECT:
          // For OBJECT type, we need to convert based on the actual type of the value. This can happen when the scalar
          // function returns Object type, e.g. cast function.
          if (value instanceof Boolean) {
            return ((boolean) value) ? 1 : 0;
          }
          if (value instanceof Timestamp) {
            return ((Timestamp) value).getTime();
          }
          if (value instanceof byte[]) {
            return new ByteArray((byte[]) value);
          }
          if (value instanceof boolean[]) {
            return fromBooleanArray((boolean[]) value);
          }
          if (value instanceof Timestamp[]) {
            return fromTimestampArray((Timestamp[]) value);
          }
          return value;
        default:
          return value;
      }
    }

    /**
     * Converts the value from internal value type to the external value type.
     *
     * <p>The conversion applies to the following types:
     * <ul>
     *   <li>BOOLEAN: int -> boolean</li>
     *   <li>TIMESTAMP: long -> Timestamp</li>
     *   <li>BYTES: ByteArray -> byte[]</li>
     *   <li>BOOLEAN_ARRAY: int[] -> boolean[]</li>
     *   <li>TIMESTAMP_ARRAY: long[] -> Timestamp[]</li>
     * </ul>
     */
    public Object toExternal(Object value) {
      switch (this) {
        case BOOLEAN:
          return ((int) value) == 1;
        case TIMESTAMP:
          return new Timestamp((long) value);
        case BYTES:
          return ((ByteArray) value).getBytes();
        case BOOLEAN_ARRAY:
          return toBooleanArray((int[]) value);
        case TIMESTAMP_ARRAY:
          return toTimestampArray((long[]) value);
        default:
          return value;
      }
    }

    /**
     * Converts the given internal value to the type for external use (e.g. as UDF argument). The given value should be
     * compatible with the type. This method should be used on the reducer side of the single-stage engine.
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
        case BIG_DECIMAL:
          return (BigDecimal) value;
        case BOOLEAN:
          return ((int) value) == 1;
        case TIMESTAMP:
          return new Timestamp((long) value);
        case STRING:
        case JSON:
          return value.toString();
        case BYTES:
          return ((ByteArray) value).getBytes();
        case INT_ARRAY:
          return (int[]) value;
        case LONG_ARRAY:
          return toLongArray(value);
        case FLOAT_ARRAY:
          return (float[]) value;
        case DOUBLE_ARRAY:
          return toDoubleArray(value);
        case STRING_ARRAY:
          return (String[]) value;
        case BOOLEAN_ARRAY:
          return toBooleanArray((int[]) value);
        case TIMESTAMP_ARRAY:
          return toTimestampArray((long[]) value);
        case BYTES_ARRAY:
          return (byte[][]) value;
        case UNKNOWN: // fall through
        case OBJECT:
          return (Serializable) value;
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
        case TIMESTAMP_ARRAY:
          return formatTimestampArray((Timestamp[]) value);
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
        case BIG_DECIMAL:
          return (BigDecimal) value;
        case BOOLEAN:
          return ((int) value) == 1;
        case TIMESTAMP:
          return new Timestamp((long) value).toString();
        case STRING:
        case JSON:
          return value.toString();
        case BYTES:
          return ((ByteArray) value).toHexString();
        case INT_ARRAY:
          return (int[]) value;
        case LONG_ARRAY:
          return toLongArray(value);
        case FLOAT_ARRAY:
          return (float[]) value;
        case DOUBLE_ARRAY:
          return toDoubleArray(value);
        case STRING_ARRAY:
          return (String[]) value;
        case BOOLEAN_ARRAY:
          return toBooleanArray((int[]) value);
        case TIMESTAMP_ARRAY:
          return formatTimestampArray((long[]) value);
        case BYTES_ARRAY:
          return (byte[][]) value;
        default:
          throw new IllegalStateException(String.format("Cannot convert and format: '%s' to type: %s", value, this));
      }
    }

    private static double[] toDoubleArray(Object value) {
      if (value instanceof double[]) {
        return (double[]) value;
      } else if (value instanceof DoubleArrayList) {
        // For HistogramAggregationFunction
        return ((DoubleArrayList) value).elements();
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
    }

    private static long[] toLongArray(Object value) {
      if (value instanceof long[]) {
        return (long[]) value;
      } else if (value instanceof LongArrayList) {
        // For FunnelCountAggregationFunction
        return ((LongArrayList) value).elements();
      } else {
        int[] intValues = (int[]) value;
        int length = intValues.length;
        long[] longValues = new long[length];
        for (int i = 0; i < length; i++) {
          longValues[i] = intValues[i];
        }
        return longValues;
      }
    }

    private static boolean[] toBooleanArray(int[] intArray) {
      int length = intArray.length;
      boolean[] booleanArray = new boolean[length];
      for (int i = 0; i < length; i++) {
        booleanArray[i] = intArray[i] == 1;
      }
      return booleanArray;
    }

    private static int[] fromBooleanArray(boolean[] booleanArray) {
      int length = booleanArray.length;
      int[] intArray = new int[length];
      for (int i = 0; i < length; i++) {
        intArray[i] = booleanArray[i] ? 1 : 0;
      }
      return intArray;
    }

    private static Timestamp[] toTimestampArray(long[] longArray) {
      int length = longArray.length;
      Timestamp[] timestampArray = new Timestamp[length];
      for (int i = 0; i < length; i++) {
        timestampArray[i] = new Timestamp(longArray[i]);
      }
      return timestampArray;
    }

    private static long[] fromTimestampArray(Timestamp[] timestampArray) {
      int length = timestampArray.length;
      long[] longArray = new long[length];
      for (int i = 0; i < length; i++) {
        longArray[i] = timestampArray[i].getTime();
      }
      return longArray;
    }

    private static String[] formatTimestampArray(long[] longArray) {
      int length = longArray.length;
      String[] formattedTimestampArray = new String[length];
      for (int i = 0; i < length; i++) {
        formattedTimestampArray[i] = new Timestamp(longArray[i]).toString();
      }
      return formattedTimestampArray;
    }

    private static String[] formatTimestampArray(Timestamp[] timestampArray) {
      int length = timestampArray.length;
      String[] formattedTimestampArray = new String[length];
      for (int i = 0; i < length; i++) {
        formattedTimestampArray[i] = timestampArray[i].toString();
      }
      return formattedTimestampArray;
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
        case BIG_DECIMAL:
          return BIG_DECIMAL;
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
        case UNKNOWN:
          return UNKNOWN;
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
        case BOOLEAN:
          return BOOLEAN_ARRAY;
        case TIMESTAMP:
          return TIMESTAMP_ARRAY;
        case STRING:
          return STRING_ARRAY;
        case BYTES:
          return BYTES_ARRAY;
        default:
          throw new IllegalStateException("Unsupported data type: " + dataType);
      }
    }
  }
}
