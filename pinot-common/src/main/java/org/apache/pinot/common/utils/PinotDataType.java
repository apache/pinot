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

import java.sql.Timestamp;
import java.util.Base64;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.TimestampUtils;


/**
 *  The <code>PinotDataType</code> enum represents the data type of a value in a row from recordReader and provides
 *  utility methods to convert value across types if applicable.
 *  <p>We don't use <code>PinotDataType</code> to maintain type information, but use it to help organize the data and
 *  use {@link DataType} to maintain type information separately across various readers.
 *  <p>NOTE:
 *  <ul>
 *    <li>We will silently lose information if a conversion causes us to do so (e.g. DOUBLE to INT)</li>
 *    <li>We will throw exception if a conversion is not possible (e.g. BOOLEAN to INT).</li>
 *    <li>We will throw exception if the conversion throws exception (e.g. "foo" -> INT)</li>
 *  </ul>
 */
public enum PinotDataType {

  /**
   * When converting from BOOLEAN to other types:
   * - Numeric types:
   *   - true -> 1
   *   - false -> 0
   * - String:
   *   - true -> "true"
   *   - false -> "false"
   *
   * When converting to BOOLEAN from other types:
   * - Numeric types:
   *   - 0 -> false
   *   - Others -> true
   * - String:
   *   - "true" (case-insensitive) -> true
   *   - "1" -> true (for backward-compatibility where we used to use integer 1 to represent true)
   *   - Others ->  false
   */
  BOOLEAN {
    @Override
    public int toInt(Object value) {
      return ((Boolean) value) ? 1 : 0;
    }

    @Override
    public long toLong(Object value) {
      return ((Boolean) value) ? 1L : 0L;
    }

    @Override
    public float toFloat(Object value) {
      return ((Boolean) value) ? 1f : 0f;
    }

    @Override
    public double toDouble(Object value) {
      return ((Boolean) value) ? 1d : 0d;
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Boolean) value;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BOOLEAN to TIMESTAMP");
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BOOLEAN to BYTES");
    }

    @Override
    public Boolean convert(Object value, PinotDataType sourceType) {
      return sourceType.toBoolean(value);
    }

    @Override
    public Integer toInternal(Object value) {
      return ((Boolean) value) ? 1 : 0;
    }
  },

  BYTE {
    @Override
    public int toInt(Object value) {
      return ((Byte) value).intValue();
    }

    @Override
    public long toLong(Object value) {
      return ((Byte) value).longValue();
    }

    @Override
    public float toFloat(Object value) {
      return ((Byte) value).floatValue();
    }

    @Override
    public double toDouble(Object value) {
      return ((Byte) value).doubleValue();
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Byte) value != 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BOOLEAN to TIMESTAMP");
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTE to BYTES");
    }
  },

  CHARACTER {
    @Override
    public int toInt(Object value) {
      return (int) ((Character) value);
    }

    @Override
    public long toLong(Object value) {
      return (long) ((Character) value);
    }

    @Override
    public float toFloat(Object value) {
      return (float) ((Character) value);
    }

    @Override
    public double toDouble(Object value) {
      return (double) ((Character) value);
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Character) value != 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from CHARACTER to TIMESTAMP");
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from CHARACTER to BYTES");
    }
  },

  SHORT {
    @Override
    public int toInt(Object value) {
      return ((Short) value).intValue();
    }

    @Override
    public long toLong(Object value) {
      return ((Short) value).longValue();
    }

    @Override
    public float toFloat(Object value) {
      return ((Short) value).floatValue();
    }

    @Override
    public double toDouble(Object value) {
      return ((Short) value).doubleValue();
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Short) value != 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from SHORT to TIMESTAMP");
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from SHORT to BYTES");
    }
  },

  INTEGER {
    @Override
    public int toInt(Object value) {
      return (Integer) value;
    }

    @Override
    public long toLong(Object value) {
      return ((Integer) value).longValue();
    }

    @Override
    public float toFloat(Object value) {
      return ((Integer) value).floatValue();
    }

    @Override
    public double toDouble(Object value) {
      return ((Integer) value).doubleValue();
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Integer) value != 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from INTEGER to TIMESTAMP");
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from INTEGER to BYTES");
    }

    @Override
    public Integer convert(Object value, PinotDataType sourceType) {
      return sourceType.toInt(value);
    }
  },

  LONG {
    @Override
    public int toInt(Object value) {
      return ((Long) value).intValue();
    }

    @Override
    public long toLong(Object value) {
      return (Long) value;
    }

    @Override
    public float toFloat(Object value) {
      return ((Long) value).floatValue();
    }

    @Override
    public double toDouble(Object value) {
      return ((Long) value).doubleValue();
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Long) value != 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      return new Timestamp((Long) value);
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from LONG to BYTES");
    }

    @Override
    public Long convert(Object value, PinotDataType sourceType) {
      return sourceType.toLong(value);
    }
  },

  FLOAT {
    @Override
    public int toInt(Object value) {
      return ((Float) value).intValue();
    }

    @Override
    public long toLong(Object value) {
      return ((Float) value).longValue();
    }

    @Override
    public float toFloat(Object value) {
      return (Float) value;
    }

    @Override
    public double toDouble(Object value) {
      return ((Float) value).doubleValue();
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Float) value != 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from FLOAT to TIMESTAMP");
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from FLOAT to BYTES");
    }

    @Override
    public Float convert(Object value, PinotDataType sourceType) {
      return sourceType.toFloat(value);
    }
  },

  DOUBLE {
    @Override
    public int toInt(Object value) {
      return ((Double) value).intValue();
    }

    @Override
    public long toLong(Object value) {
      return ((Double) value).longValue();
    }

    @Override
    public float toFloat(Object value) {
      return ((Double) value).floatValue();
    }

    @Override
    public double toDouble(Object value) {
      return (Double) value;
    }

    @Override
    public boolean toBoolean(Object value) {
      return (Double) value != 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      return new Timestamp(((Double) value).longValue());
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from DOUBLE to BYTES");
    }

    @Override
    public Double convert(Object value, PinotDataType sourceType) {
      return sourceType.toDouble(value);
    }
  },

  /**
   * When converting from TIMESTAMP to other types:
   * - LONG/DOUBLE: millis since epoch value
   * - String: SQL timestamp format (e.g. "2021-01-01 01:01:01.001")
   *
   * When converting to TIMESTAMP from other types:
   * - LONG/DOUBLE: read long value as millis since epoch
   * - String:
   *   - SQL timestamp format (e.g. "2021-01-01 01:01:01.001")
   *   - Millis since epoch value (e.g. "1609491661001")
   */
  TIMESTAMP {
    @Override
    public int toInt(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP to INTEGER");
    }

    @Override
    public long toLong(Object value) {
      return ((Timestamp) value).getTime();
    }

    @Override
    public float toFloat(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP to FLOAT");
    }

    @Override
    public double toDouble(Object value) {
      return ((Timestamp) value).getTime();
    }

    @Override
    public boolean toBoolean(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP to BOOLEAN");
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      return (Timestamp) value;
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from TIMESTAMP to BYTES");
    }

    @Override
    public Timestamp convert(Object value, PinotDataType sourceType) {
      return sourceType.toTimestamp(value);
    }

    @Override
    public Long toInternal(Object value) {
      return ((Timestamp) value).getTime();
    }
  },

  STRING {
    @Override
    public int toInt(Object value) {
      return Integer.parseInt(value.toString().trim());
    }

    @Override
    public long toLong(Object value) {
      return Long.parseLong(value.toString().trim());
    }

    @Override
    public float toFloat(Object value) {
      // NOTE: No need to trim here because Float.valueOf() will trim the string
      return Float.parseFloat(value.toString());
    }

    @Override
    public double toDouble(Object value) {
      // NOTE: No need to trim here because Double.valueOf() will trim the string
      return Double.parseDouble(value.toString());
    }

    @Override
    public boolean toBoolean(Object value) {
      return BooleanUtils.toBoolean(value.toString().trim());
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      return TimestampUtils.toTimestamp(value.toString().trim());
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      return BytesUtils.toBytes(value.toString().trim());
    }

    @Override
    public String convert(Object value, PinotDataType sourceType) {
      return sourceType.toString(value);
    }
  },

  JSON {
    @Override
    public int toInt(Object value) {
      return Integer.parseInt(value.toString().trim());
    }

    @Override
    public long toLong(Object value) {
      return Long.parseLong(value.toString().trim());
    }

    @Override
    public float toFloat(Object value) {
      return Float.parseFloat(value.toString().trim());
    }

    @Override
    public double toDouble(Object value) {
      return Double.parseDouble(value.toString().trim());
    }

    @Override
    public boolean toBoolean(Object value) {
      return Boolean.parseBoolean(value.toString().trim());
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      return (value instanceof Long) ? new Timestamp(((Long) value).longValue())
          : Timestamp.valueOf(value.toString().trim());
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      if (value instanceof String) {
        // Assume that input JSON string value is Base64 encoded.
        try {
          return Base64.getDecoder().decode(((String) value).getBytes("UTF-8"));
        } catch (Exception e) {
          throw new RuntimeException(
              "Unable to convert JSON base64 encoded string value to BYTES. Input value: " + value, e);
        }
      }

      throw new UnsupportedOperationException("Cannot convert non base64 encoded string value from JSON to BYTES");
    }

    @Override
    public String convert(Object value, PinotDataType sourceType) {
      return sourceType.toJson(value);
    }
  },

  BYTES {
    @Override
    public int toInt(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTES to INTEGER");
    }

    @Override
    public long toLong(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTES to LONG");
    }

    @Override
    public float toFloat(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTES to FLOAT");
    }

    @Override
    public double toDouble(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTES to DOUBLE");
    }

    @Override
    public boolean toBoolean(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTES to BOOLEAN");
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from BYTES to TIMESTAMP");
    }

    @Override
    public String toString(Object value) {
      return BytesUtils.toHexString((byte[]) value);
    }

    @Override
    public byte[] toBytes(Object value) {
      return (byte[]) value;
    }

    @Override
    public Object convert(Object value, PinotDataType sourceType) {
      return sourceType.toBytes(value);
    }
  },

  OBJECT {
    @Override
    public int toInt(Object value) {
      return ((Number) value).intValue();
    }

    @Override
    public long toLong(Object value) {
      return ((Number) value).longValue();
    }

    @Override
    public float toFloat(Object value) {
      return ((Number) value).floatValue();
    }

    @Override
    public double toDouble(Object value) {
      return ((Number) value).doubleValue();
    }

    @Override
    public boolean toBoolean(Object value) {
      return ((Number) value).intValue() > 0;
    }

    @Override
    public Timestamp toTimestamp(Object value) {
      return new Timestamp(((Number) value).longValue());
    }

    @Override
    public String toString(Object value) {
      return value.toString();
    }

    @Override
    public byte[] toBytes(Object value) {
      throw new UnsupportedOperationException("Cannot convert value from OBJECT to BYTES");
    }
  },

  BYTE_ARRAY {
    @Override
    public byte[] toBytes(Object value) {
      Object[] valueArray = (Object[]) value;
      int length = valueArray.length;
      byte[] bytes = new byte[length];
      for (int i = 0; i < length; i++) {
        bytes[i] = (Byte) valueArray[i];
      }
      return bytes;
    }
  },

  CHARACTER_ARRAY,

  SHORT_ARRAY,

  /*
    NOTE:
      Primitive array is used in query execution, query response, scalar function arguments and return values.
      Object array is used in GenericRow for data ingestion.
      We need to keep them separately because they cannot automatically cast to the other type.
   */

  PRIMITIVE_INT_ARRAY {
    @Override
    public int[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toPrimitiveIntArray(value);
    }
  },

  INTEGER_ARRAY {
    @Override
    public Integer[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toIntegerArray(value);
    }
  },

  PRIMITIVE_LONG_ARRAY {
    @Override
    public long[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toPrimitiveLongArray(value);
    }
  },

  LONG_ARRAY {
    @Override
    public Long[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toLongArray(value);
    }
  },

  PRIMITIVE_FLOAT_ARRAY {
    @Override
    public float[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toPrimitiveFloatArray(value);
    }
  },

  FLOAT_ARRAY {
    @Override
    public Float[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toFloatArray(value);
    }
  },

  PRIMITIVE_DOUBLE_ARRAY {
    @Override
    public double[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toPrimitiveDoubleArray(value);
    }
  },

  DOUBLE_ARRAY {
    @Override
    public Double[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toDoubleArray(value);
    }
  },

  STRING_ARRAY {
    @Override
    public String[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toStringArray(value);
    }
  },

  OBJECT_ARRAY;

  /**
   * NOTE: override toInt(), toLong(), toFloat(), toDouble(), toBoolean(), toTimestamp(), toString(), toJson(), and
   * toBytes() for single-value types.
   */

  public int toInt(Object value) {
    return getSingleValueType().toInt(toObjectArray(value)[0]);
  }

  public long toLong(Object value) {
    return getSingleValueType().toLong(toObjectArray(value)[0]);
  }

  public float toFloat(Object value) {
    return getSingleValueType().toFloat(toObjectArray(value)[0]);
  }

  public double toDouble(Object value) {
    return getSingleValueType().toDouble(toObjectArray(value)[0]);
  }

  public boolean toBoolean(Object value) {
    return getSingleValueType().toBoolean(((Object[]) value)[0]);
  }

  public Timestamp toTimestamp(Object value) {
    return getSingleValueType().toTimestamp(((Object[]) value)[0]);
  }

  public String toString(Object value) {
    return getSingleValueType().toString(toObjectArray(value)[0]);
  }


  public String toJson(Object value) {
    if (value instanceof String) {
      try {
        return JsonUtils.stringToJsonNode((String) value).toString();
      } catch (Exception e) {
        throw new RuntimeException("Unable to convert String into JSON. Input value: " + value, e);
      }
    } else {
      try {
        return JsonUtils.objectToString(value).toString();
      } catch (Exception e) {
        throw new RuntimeException("Unable to convert " + value.getClass().getCanonicalName() + " to JSON. Input value: " + value, e);
      }
    }
  }

  public byte[] toBytes(Object value) {
    return getSingleValueType().toBytes(toObjectArray(value)[0]);
  }

  public int[] toPrimitiveIntArray(Object value) {
    if (value instanceof int[]) {
      return (int[]) value;
    }
    if (isSingleValue()) {
      return new int[]{toInt(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      int[] intArray = new int[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        intArray[i] = singleValueType.toInt(valueArray[i]);
      }
      return intArray;
    }
  }

  public Integer[] toIntegerArray(Object value) {
    if (value instanceof Integer[]) {
      return (Integer[]) value;
    }
    if (isSingleValue()) {
      return new Integer[]{toInt(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      Integer[] integerArray = new Integer[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        integerArray[i] = singleValueType.toInt(valueArray[i]);
      }
      return integerArray;
    }
  }

  public long[] toPrimitiveLongArray(Object value) {
    if (value instanceof long[]) {
      return (long[]) value;
    }
    if (isSingleValue()) {
      return new long[]{toLong(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      long[] longArray = new long[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        longArray[i] = singleValueType.toLong(valueArray[i]);
      }
      return longArray;
    }
  }

  public Long[] toLongArray(Object value) {
    if (value instanceof Long[]) {
      return (Long[]) value;
    }
    if (isSingleValue()) {
      return new Long[]{toLong(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      Long[] longArray = new Long[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        longArray[i] = singleValueType.toLong(valueArray[i]);
      }
      return longArray;
    }
  }

  public float[] toPrimitiveFloatArray(Object value) {
    if (value instanceof float[]) {
      return (float[]) value;
    }
    if (isSingleValue()) {
      return new float[]{toFloat(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      float[] floatArray = new float[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        floatArray[i] = singleValueType.toFloat(valueArray[i]);
      }
      return floatArray;
    }
  }

  public Float[] toFloatArray(Object value) {
    if (value instanceof Float[]) {
      return (Float[]) value;
    }
    if (isSingleValue()) {
      return new Float[]{toFloat(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      Float[] floatArray = new Float[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        floatArray[i] = singleValueType.toFloat(valueArray[i]);
      }
      return floatArray;
    }
  }

  public double[] toPrimitiveDoubleArray(Object value) {
    if (value instanceof double[]) {
      return (double[]) value;
    }
    if (isSingleValue()) {
      return new double[]{toDouble(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      double[] doubleArray = new double[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        doubleArray[i] = singleValueType.toDouble(valueArray[i]);
      }
      return doubleArray;
    }
  }

  public Double[] toDoubleArray(Object value) {
    if (value instanceof Double[]) {
      return (Double[]) value;
    }
    if (isSingleValue()) {
      return new Double[]{toDouble(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      Double[] doubleArray = new Double[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        doubleArray[i] = singleValueType.toDouble(valueArray[i]);
      }
      return doubleArray;
    }
  }

  public String[] toStringArray(Object value) {
    if (value instanceof String[]) {
      return (String[]) value;
    }
    if (isSingleValue()) {
      return new String[]{toString(value)};
    } else {
      Object[] valueArray = toObjectArray(value);
      int length = valueArray.length;
      String[] stringArray = new String[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        stringArray[i] = singleValueType.toString(valueArray[i]);
      }
      return stringArray;
    }
  }

  private static Object[] toObjectArray(Object array) {
    Class<?> componentType = array.getClass().getComponentType();
    if (componentType.isPrimitive()) {
      if (componentType == int.class) {
        return ArrayUtils.toObject((int[]) array);
      }
      if (componentType == long.class) {
        return ArrayUtils.toObject((long[]) array);
      }
      if (componentType == float.class) {
        return ArrayUtils.toObject((float[]) array);
      }
      if (componentType == double.class) {
        return ArrayUtils.toObject((double[]) array);
      }
      throw new UnsupportedOperationException("Unsupported primitive array type: " + componentType);
    } else {
      return (Object[]) array;
    }
  }

  public Object convert(Object value, PinotDataType sourceType) {
    throw new UnsupportedOperationException("Cannot convert value from " + sourceType + " to " + this);
  }

  /**
   * Converts to the internal representation of the value.
   * <ul>
   *   <li>BOOLEAN -> int</li>
   *   <li>TIMESTAMP -> long</li>
   * </ul>
   */
  public Object toInternal(Object value) {
    return value;
  }

  public boolean isSingleValue() {
    return this.ordinal() <= OBJECT.ordinal();
  }

  public PinotDataType getSingleValueType() {
    switch (this) {
      case BYTE_ARRAY:
        return BYTE;
      case CHARACTER_ARRAY:
        return CHARACTER;
      case SHORT_ARRAY:
        return SHORT;
      case PRIMITIVE_INT_ARRAY:
      case INTEGER_ARRAY:
        return INTEGER;
      case PRIMITIVE_LONG_ARRAY:
      case LONG_ARRAY:
        return LONG;
      case PRIMITIVE_FLOAT_ARRAY:
      case FLOAT_ARRAY:
        return FLOAT;
      case PRIMITIVE_DOUBLE_ARRAY:
      case DOUBLE_ARRAY:
        return DOUBLE;
      case STRING_ARRAY:
        return STRING;
      case OBJECT_ARRAY:
        return OBJECT;
      default:
        throw new IllegalStateException("There is no single-value type for " + this);
    }
  }

  /**
   * Returns the {@link PinotDataType} for the given {@link FieldSpec} for data ingestion purpose. Returns object array
   * type for multi-valued types.
   * TODO: Add MV support for BOOLEAN, TIMESTAMP, BYTES
   */
  public static PinotDataType getPinotDataTypeForIngestion(FieldSpec fieldSpec) {
    DataType dataType = fieldSpec.getDataType();
    switch (dataType) {
      case INT:
        return fieldSpec.isSingleValueField() ? PinotDataType.INTEGER : PinotDataType.INTEGER_ARRAY;
      case LONG:
        return fieldSpec.isSingleValueField() ? PinotDataType.LONG : PinotDataType.LONG_ARRAY;
      case FLOAT:
        return fieldSpec.isSingleValueField() ? PinotDataType.FLOAT : PinotDataType.FLOAT_ARRAY;
      case DOUBLE:
        return fieldSpec.isSingleValueField() ? PinotDataType.DOUBLE : PinotDataType.DOUBLE_ARRAY;
      case BOOLEAN:
        if (fieldSpec.isSingleValueField()) {
          return PinotDataType.BOOLEAN;
        } else {
          throw new IllegalStateException("There is no multi-value type for BOOLEAN");
        }
      case TIMESTAMP:
        if (fieldSpec.isSingleValueField()) {
          return PinotDataType.TIMESTAMP;
        } else {
          throw new IllegalStateException("There is no multi-value type for TIMESTAMP");
        }
      case JSON:
        if (fieldSpec.isSingleValueField()) {
          return PinotDataType.JSON;
        } else {
          throw new IllegalStateException("There is no multi-value type for JSON");
        }
      case STRING:
        return fieldSpec.isSingleValueField() ? PinotDataType.STRING : PinotDataType.STRING_ARRAY;
      case BYTES:
        if (fieldSpec.isSingleValueField()) {
          return PinotDataType.BYTES;
        } else {
          throw new IllegalStateException("There is no multi-value type for BYTES");
        }
      default:
        throw new UnsupportedOperationException(
            "Unsupported data type: " + dataType + " in field: " + fieldSpec.getName());
    }
  }

  /**
   * Returns the {@link PinotDataType} for the given {@link ColumnDataType} for query execution purpose. Returns
   * primitive array type for multi-valued types.
   */
  public static PinotDataType getPinotDataTypeForExecution(ColumnDataType columnDataType) {
    switch (columnDataType) {
      case INT:
        return INTEGER;
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
      case INT_ARRAY:
        return PRIMITIVE_INT_ARRAY;
      case LONG_ARRAY:
        return PRIMITIVE_LONG_ARRAY;
      case FLOAT_ARRAY:
        return PRIMITIVE_FLOAT_ARRAY;
      case DOUBLE_ARRAY:
        return PRIMITIVE_DOUBLE_ARRAY;
      case STRING_ARRAY:
        return STRING_ARRAY;
      default:
        throw new IllegalStateException("Cannot convert ColumnDataType: " + columnDataType + " to PinotDataType");
    }
  }
}
