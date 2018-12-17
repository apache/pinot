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
package com.linkedin.pinot.core.data.recordtransformer;

import com.linkedin.pinot.common.data.FieldSpec;


/**
 *  The <code>PinotDataType</code> enum represents the data type of a value in a row from recordReader and provides
 *  utility methods to convert value across types if applicable.
 *  <p>We don't use <code>PinotDataType</code> to maintain type information, but use it to help organize the data and
 *  use {@link FieldSpec.DataType} to maintain type information separately across various readers.
 *  <p>NOTE:
 *  <ul>
 *    <li>We will silently lose information if a conversion causes us to do so (e.g. DOUBLE to INT)</li>
 *    <li>We will throw exception if a conversion is not possible (e.g. BOOLEAN to INT).</li>
 *    <li>We will throw exception if the conversion throws exception (e.g. "foo" -> INT)</li>
 *  </ul>
 */
public enum PinotDataType {
  BOOLEAN {
    @Override
    public Integer toInteger(Object value) {
      throw new UnsupportedOperationException("Cannot convert value: " + value + " from: BOOLEAN to: INTEGER");
    }

    @Override
    public Long toLong(Object value) {
      throw new UnsupportedOperationException("Cannot convert value: " + value + " from: BOOLEAN to: LONG");
    }

    @Override
    public Float toFloat(Object value) {
      throw new UnsupportedOperationException("Cannot convert value: " + value + " from: BOOLEAN to: FLOAT");
    }

    @Override
    public Double toDouble(Object value) {
      throw new UnsupportedOperationException("Cannot convert value: " + value + " from: BOOLEAN to: DOUBLE");
    }
  },

  BYTE {
    @Override
    public Integer toInteger(Object value) {
      return ((Byte) value).intValue();
    }

    @Override
    public Long toLong(Object value) {
      return ((Byte) value).longValue();
    }

    @Override
    public Float toFloat(Object value) {
      return ((Byte) value).floatValue();
    }

    @Override
    public Double toDouble(Object value) {
      return ((Byte) value).doubleValue();
    }
  },

  BYTES {
    // TODO: Support conversion from BYTES to other data types
  },

  CHARACTER {
    @Override
    public Integer toInteger(Object value) {
      return (int) ((Character) value);
    }

    @Override
    public Long toLong(Object value) {
      return (long) ((Character) value);
    }

    @Override
    public Float toFloat(Object value) {
      return (float) ((Character) value);
    }

    @Override
    public Double toDouble(Object value) {
      return (double) ((Character) value);
    }
  },

  SHORT {
    @Override
    public Integer toInteger(Object value) {
      return ((Short) value).intValue();
    }

    @Override
    public Long toLong(Object value) {
      return ((Short) value).longValue();
    }

    @Override
    public Float toFloat(Object value) {
      return ((Short) value).floatValue();
    }

    @Override
    public Double toDouble(Object value) {
      return ((Short) value).doubleValue();
    }
  },

  INTEGER {
    @Override
    public Integer toInteger(Object value) {
      return (Integer) value;
    }

    @Override
    public Long toLong(Object value) {
      return ((Integer) value).longValue();
    }

    @Override
    public Float toFloat(Object value) {
      return ((Integer) value).floatValue();
    }

    @Override
    public Double toDouble(Object value) {
      return ((Integer) value).doubleValue();
    }

    @Override
    public Integer convert(Object value, PinotDataType sourceType) {
      return sourceType.toInteger(value);
    }
  },

  LONG {
    @Override
    public Integer toInteger(Object value) {
      return ((Long) value).intValue();
    }

    @Override
    public Long toLong(Object value) {
      return (Long) value;
    }

    @Override
    public Float toFloat(Object value) {
      return ((Long) value).floatValue();
    }

    @Override
    public Double toDouble(Object value) {
      return ((Long) value).doubleValue();
    }

    @Override
    public Long convert(Object value, PinotDataType sourceType) {
      return sourceType.toLong(value);
    }
  },

  FLOAT {
    @Override
    public Integer toInteger(Object value) {
      return ((Float) value).intValue();
    }

    @Override
    public Long toLong(Object value) {
      return ((Float) value).longValue();
    }

    @Override
    public Float toFloat(Object value) {
      return (Float) value;
    }

    @Override
    public Double toDouble(Object value) {
      return ((Float) value).doubleValue();
    }

    @Override
    public Float convert(Object value, PinotDataType sourceType) {
      return sourceType.toFloat(value);
    }
  },

  DOUBLE {
    @Override
    public Integer toInteger(Object value) {
      return ((Double) value).intValue();
    }

    @Override
    public Long toLong(Object value) {
      return ((Double) value).longValue();
    }

    @Override
    public Float toFloat(Object value) {
      return ((Double) value).floatValue();
    }

    @Override
    public Double toDouble(Object value) {
      return (Double) value;
    }

    @Override
    public Double convert(Object value, PinotDataType sourceType) {
      return sourceType.toDouble(value);
    }
  },

  STRING {
    @Override
    public Integer toInteger(Object value) {
      return Integer.parseInt((String) value);
    }

    @Override
    public Long toLong(Object value) {
      return Long.parseLong((String) value);
    }

    @Override
    public Float toFloat(Object value) {
      return Float.parseFloat((String) value);
    }

    @Override
    public Double toDouble(Object value) {
      return Double.parseDouble((String) value);
    }

    @Override
    public String convert(Object value, PinotDataType sourceType) {
      return sourceType.toString(value);
    }
  },

  OBJECT {
    @Override
    public Integer toInteger(Object value) {
      throw new UnsupportedOperationException("Cannot convert value: " + value + " from: OBJECT to: INTEGER");
    }

    @Override
    public Long toLong(Object value) {
      throw new UnsupportedOperationException("Cannot convert value: " + value + " from: OBJECT to: LONG");
    }

    @Override
    public Float toFloat(Object value) {
      throw new UnsupportedOperationException("Cannot convert value: " + value + " from: OBJECT to: FLOAT");
    }

    @Override
    public Double toDouble(Object value) {
      throw new UnsupportedOperationException("Cannot convert value: " + value + " from: OBJECT to: DOUBLE");
    }
  },

  BYTE_ARRAY,

  CHARACTER_ARRAY,

  SHORT_ARRAY,

  INTEGER_ARRAY {
    @Override
    public Integer[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toIntegerArray(value);
    }
  },

  LONG_ARRAY {
    @Override
    public Long[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toLongArray(value);
    }
  },

  FLOAT_ARRAY {
    @Override
    public Float[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toFloatArray(value);
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

  public Integer toInteger(Object value) {
    return getSingleValueType().toInteger(((Object[]) value)[0]);
  }

  public Long toLong(Object value) {
    return getSingleValueType().toLong(((Object[]) value)[0]);
  }

  public Float toFloat(Object value) {
    return getSingleValueType().toFloat(((Object[]) value)[0]);
  }

  public Double toDouble(Object value) {
    return getSingleValueType().toDouble(((Object[]) value)[0]);
  }

  public String toString(Object value) {
    if (isSingleValue()) {
      return value.toString();
    } else {
      return ((Object[]) value)[0].toString();
    }
  }

  // TODO: Support toBytes() for all data types
  public byte[] toBytes(Object value) {
    throw new UnsupportedOperationException("Cannot convert value: " + value + " form: " + this + " to: BYTES");
  }

  public Integer[] toIntegerArray(Object value) {
    if (isSingleValue()) {
      return new Integer[]{toInteger(value)};
    } else {
      Object[] valueArray = (Object[]) value;
      int length = valueArray.length;
      Integer[] integerArray = new Integer[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        integerArray[i] = singleValueType.toInteger(valueArray[i]);
      }
      return integerArray;
    }
  }

  public Long[] toLongArray(Object value) {
    if (isSingleValue()) {
      return new Long[]{toLong(value)};
    } else {
      Object[] valueArray = (Object[]) value;
      int length = valueArray.length;
      Long[] longArray = new Long[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        longArray[i] = singleValueType.toLong(valueArray[i]);
      }
      return longArray;
    }
  }

  public Float[] toFloatArray(Object value) {
    if (isSingleValue()) {
      return new Float[]{toFloat(value)};
    } else {
      Object[] valueArray = (Object[]) value;
      int length = valueArray.length;
      Float[] floatArray = new Float[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        floatArray[i] = singleValueType.toFloat(valueArray[i]);
      }
      return floatArray;
    }
  }

  public Double[] toDoubleArray(Object value) {
    if (isSingleValue()) {
      return new Double[]{toDouble(value)};
    } else {
      Object[] valueArray = (Object[]) value;
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
    if (isSingleValue()) {
      return new String[]{toString(value)};
    } else {
      Object[] valueArray = (Object[]) value;
      int length = valueArray.length;
      String[] stringArray = new String[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        stringArray[i] = singleValueType.toString(valueArray[i]);
      }
      return stringArray;
    }
  }

  public Object convert(Object value, PinotDataType sourceType) {
    throw new UnsupportedOperationException("Cannot convert value: " + value + " form: " + sourceType + " to: " + this);
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
      case INTEGER_ARRAY:
        return INTEGER;
      case LONG_ARRAY:
        return LONG;
      case FLOAT_ARRAY:
        return FLOAT;
      case DOUBLE_ARRAY:
        return DOUBLE;
      case STRING_ARRAY:
        return STRING;
      case OBJECT_ARRAY:
        return OBJECT;
      default:
        throw new UnsupportedOperationException("Cannot get single-value type for: " + this);
    }
  }

  public static PinotDataType getPinotDataType(FieldSpec fieldSpec) {
    FieldSpec.DataType dataType = fieldSpec.getDataType();
    switch (dataType) {
      case INT:
        return fieldSpec.isSingleValueField() ? PinotDataType.INTEGER : PinotDataType.INTEGER_ARRAY;
      case LONG:
        return fieldSpec.isSingleValueField() ? PinotDataType.LONG : PinotDataType.LONG_ARRAY;
      case FLOAT:
        return fieldSpec.isSingleValueField() ? PinotDataType.FLOAT : PinotDataType.FLOAT_ARRAY;
      case DOUBLE:
        return fieldSpec.isSingleValueField() ? PinotDataType.DOUBLE : PinotDataType.DOUBLE_ARRAY;
      case STRING:
        return fieldSpec.isSingleValueField() ? PinotDataType.STRING : PinotDataType.STRING_ARRAY;
      case BYTES:
        if (fieldSpec.isSingleValueField()) {
          return PinotDataType.BYTES;
        } else {
          throw new UnsupportedOperationException("Unsupported multi-valued type: BYTES");
        }
      default:
        throw new UnsupportedOperationException(
            "Unsupported data type: " + dataType + " in field: " + fieldSpec.getName());
    }
  }
}
