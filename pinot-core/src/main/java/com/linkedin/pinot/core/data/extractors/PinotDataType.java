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
package com.linkedin.pinot.core.data.extractors;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;


/**
 *  The <code>PinotDataType</code> enum represents the data type of a value in a row from recordReader and provides
 *  utility methods to convert value across types if applicable.
 *  <p>
 *  We don't use <code>PinotDataType</code> to maintain type information, but use it to help organize the data and use
 *  {@link com.linkedin.pinot.common.data.FieldSpec.DataType} to maintain type information separately across
 *  various readers.
 *  <p>NOTE:
 *  <p>- a. we will silently lose information if a conversion causes us to do so (e.g. Integer to Byte).
 *  <p>- b. We will throw exceptions if a conversion is not possible (e.g. Boolean to Byte).
 *  <p>- c. we will throw exceptions if the conversion throw exceptions (e.g. String -> Short, where parsing string
 *  throw exceptions)
 */
public enum PinotDataType {
  BOOLEAN {
    @Override
    public Boolean toBoolean(Object value) {
      return (Boolean) value;
    }

    @Override
    public Byte toByte(Object value) {
      throw new UnsupportedOperationException("Cannot convert value: " + value + " from: BOOLEAN to: BYTE");
    }

    @Override
    public Character toCharacter(Object value) {
      throw new UnsupportedOperationException("Cannot convert value: " + value + " from: BOOLEAN to: CHARACTER");
    }

    @Override
    public Short toShort(Object value) {
      throw new UnsupportedOperationException("Cannot convert value: " + value + " from: BOOLEAN to: SHORT");
    }

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

    @Override
    public Boolean convert(Object value, PinotDataType sourceType) {
      return sourceType.toBoolean(value);
    }
  },

  BYTE {
    @Override
    public Byte toByte(Object value) {
      return (Byte) value;
    }

    @Override
    public Character toCharacter(Object value) {
      return (char) ((Byte) value).shortValue();
    }

    @Override
    public Short toShort(Object value) {
      return ((Byte) value).shortValue();
    }

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

    @Override
    public Byte convert(Object value, PinotDataType sourceType) {
      return sourceType.toByte(value);
    }
  },

  CHARACTER {
    @Override
    public Byte toByte(Object value) {
      return (byte) ((Character) value).charValue();
    }

    @Override
    public Character toCharacter(Object value) {
      return (Character) value;
    }

    @Override
    public Short toShort(Object value) {
      return (short) ((Character) value).charValue();
    }

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

    @Override
    public Character convert(Object value, PinotDataType sourceType) {
      return sourceType.toCharacter(value);
    }
  },

  SHORT {
    @Override
    public Byte toByte(Object value) {
      return ((Short) value).byteValue();
    }

    @Override
    public Character toCharacter(Object value) {
      return (char) ((Short) value).shortValue();
    }

    @Override
    public Short toShort(Object value) {
      return (Short) value;
    }

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

    @Override
    public Short convert(Object value, PinotDataType sourceType) {
      return sourceType.toShort(value);
    }
  },

  INTEGER {
    @Override
    public Byte toByte(Object value) {
      return ((Integer) value).byteValue();
    }

    @Override
    public Character toCharacter(Object value) {
      return (char) ((Integer) value).shortValue();
    }

    @Override
    public Short toShort(Object value) {
      return ((Integer) value).shortValue();
    }

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
    public Byte toByte(Object value) {
      return ((Long) value).byteValue();
    }

    @Override
    public Character toCharacter(Object value) {
      return (char) ((Long) value).shortValue();
    }

    @Override
    public Short toShort(Object value) {
      return ((Long) value).shortValue();
    }

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
    public Byte toByte(Object value) {
      return ((Float) value).byteValue();
    }

    @Override
    public Character toCharacter(Object value) {
      return (char) ((Float) value).shortValue();
    }

    @Override
    public Short toShort(Object value) {
      return ((Float) value).shortValue();
    }

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
    public Byte toByte(Object value) {
      return ((Double) value).byteValue();
    }

    @Override
    public Character toCharacter(Object value) {
      return (char) ((Double) value).shortValue();
    }

    @Override
    public Short toShort(Object value) {
      return ((Double) value).shortValue();
    }

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
    public Boolean toBoolean(Object value) {
      return Boolean.parseBoolean((String) value);
    }

    @Override
    public Byte toByte(Object value) {
      return Byte.parseByte((String) value);
    }

    @Override
    public Character toCharacter(Object value) {
      return ((String) value).charAt(0);
    }

    @Override
    public Short toShort(Object value) {
      return Short.parseShort((String) value);
    }

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
    public Byte toByte(Object value) {
      throw new UnsupportedOperationException("Cannot convert value: " + value + " from: OBJECT to: BYTE");
    }

    @Override
    public Character toCharacter(Object value) {
      throw new UnsupportedOperationException("Cannot convert value: " + value + " from: OBJECT to: CHARACTER");
    }

    @Override
    public Short toShort(Object value) {
      throw new UnsupportedOperationException("Cannot convert value: " + value + " from: OBJECT to: SHORT");
    }

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

    @Override
    public Object convert(Object value, PinotDataType sourceType) {
      throw new UnsupportedOperationException("Cannot convert value: " + value + "from: " + sourceType + " to: OBJECT");
    }
  },

  BYTE_ARRAY {
    @Override
    public Byte[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toByteArray(value);
    }
  },

  CHARACTER_ARRAY {
    @Override
    public Character[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toCharacterArray(value);
    }
  },

  SHORT_ARRAY {
    @Override
    public Short[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toShortArray(value);
    }
  },

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
    public Boolean toBoolean(Object value) {
      return STRING.toBoolean(((Object[]) value)[0]);
    }

    @Override
    public String[] convert(Object value, PinotDataType sourceType) {
      return sourceType.toStringArray(value);
    }
  },

  OBJECT_ARRAY {
    @Override
    public Object[] convert(Object value, PinotDataType sourceType) {
      throw new UnsupportedOperationException(
          "Cannot convert value: " + value + "from: " + sourceType + " to: OBJECT_ARRAY");
    }
  };

  public Boolean toBoolean(Object value) {
    throw new UnsupportedOperationException("Cannot convert value: " + value + " from: " + this + " to: BOOLEAN");
  }

  public Byte toByte(Object value) {
    return getSingleValueType().toByte(((Object[]) value)[0]);
  }

  public Character toCharacter(Object value) {
    return getSingleValueType().toCharacter(((Object[]) value)[0]);
  }

  public Short toShort(Object value) {
    return getSingleValueType().toShort(((Object[]) value)[0]);
  }

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

  public Byte[] toByteArray(Object value) {
    if (isSingleValue()) {
      return new Byte[]{toByte(value)};
    } else {
      Object[] valueArray = (Object[]) value;
      int length = valueArray.length;
      Byte[] byteArray = new Byte[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        byteArray[i] = singleValueType.toByte(valueArray[i]);
      }
      return byteArray;
    }
  }

  public Character[] toCharacterArray(Object value) {
    if (isSingleValue()) {
      return new Character[]{toCharacter(value)};
    } else {
      Object[] valueArray = (Object[]) value;
      int length = valueArray.length;
      Character[] characterArray = new Character[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        characterArray[i] = singleValueType.toCharacter(valueArray[i]);
      }
      return characterArray;
    }
  }

  public Short[] toShortArray(Object value) {
    if (isSingleValue()) {
      return new Short[]{toShort(value)};
    } else {
      Object[] valueArray = (Object[]) value;
      int length = valueArray.length;
      Short[] shortArray = new Short[length];
      PinotDataType singleValueType = getSingleValueType();
      for (int i = 0; i < length; i++) {
        shortArray[i] = singleValueType.toShort(valueArray[i]);
      }
      return shortArray;
    }
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

  public abstract Object convert(Object value, PinotDataType sourceType);

  public boolean isSingleValue() {
    return this.ordinal() < BYTE_ARRAY.ordinal();
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
      case BOOLEAN:
        Preconditions.checkArgument(fieldSpec.isSingleValueField());
        return PinotDataType.BOOLEAN;
      case BYTE:
        return fieldSpec.isSingleValueField() ? PinotDataType.BYTE : PinotDataType.BYTE_ARRAY;
      case CHAR:
        return fieldSpec.isSingleValueField() ? PinotDataType.CHARACTER : PinotDataType.CHARACTER_ARRAY;
      case SHORT:
        return fieldSpec.isSingleValueField() ? PinotDataType.SHORT : PinotDataType.SHORT_ARRAY;
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
      default:
        throw new UnsupportedOperationException(
            "Unsupported data type: " + dataType + " in field: " + fieldSpec.getName());
    }
  }
}
