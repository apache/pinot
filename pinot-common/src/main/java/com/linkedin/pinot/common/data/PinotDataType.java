/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.data;

import com.linkedin.pinot.common.data.FieldSpec;

/**
 *  A {@code PinotDataType} represents the value of a row from a recordReader
 *  and provides utility methods to convert across types if applicable.
 *  A {@code PinotDataType} does not maintain type information, but only
 *  helps organize and use Pinot type representations that may be maintained
 *  separately across various readers.
 *  a. We will return null if a conversion is not possible (e.g. Boolean  to Byte)
 *  b. we will silently lose information if a conversion causes us to do so (e.g. Integer to Byte)
 *  c. we will throw exceptions if wrong objects are given for conversion
 *  (e.g. Object -> Short, where the object is boolean type)
 *
 */
public enum PinotDataType {
  /**
   *  Value representing a boolean
   */
  BOOLEAN {
    public Boolean toBoolean(Object d) {
      return (Boolean) d;
    }
    public Byte toByte(Object d) {
      return null;
    }
    public Character toCharacter(Object d) {
      return null;
    }
    public Short toShort(Object d) {
      return null;
    }
    public Integer toInteger(Object d) {
      return null;
    }
    public Long toLong(Object d) {
      return null;
    }
    public Float toFloat(Object d) {
      return null;
    }
    public Double toDouble(Object d) {
      return null;
    }
    public String toString(Object d) {
      return d.toString();
    }
    public Byte[] toByteArray(Object d) {
      return null;
    }
    public Character[] toCharArray(Object d) {
      return null;
    }
    public Short[] toShortArray(Object d) {
      return null;
    }
    public Integer[] toIntegerArray(Object d) {
      return null;
    }
    public Long[] toLongArray(Object d) {
      return null;
    }
    public Float[] toFloatArray(Object d) {
      return null;
    }
    public Double[] toDoubleArray(Object d) {
      return null;
    }
    public String[] toStringArray(Object d) {
      return null;
    }

    public Object toObject(Object d) {
      return ((Object) d);
    }

    public Boolean convert(Object d, PinotDataType u) {
      return u.toBoolean(d);
    }

  },

  BYTE {
    public Boolean toBoolean(Object d) {
      return null;
    }
    public Byte toByte(Object d) {
      return (Byte) d;
    }

    public Character toCharacter(Object d) {
      return (Character) ((char) ((Byte) d).byteValue());
    }

    public Short toShort(Object d) {
      return (Short)  ((Byte) d).shortValue();
    }

    public Integer toInteger(Object d) {
      return (Integer)  ((Byte) d).intValue();
    }

    public Long toLong(Object d) {
      return (Long)  ((Byte) d).longValue();
    }

    public Float toFloat(Object d) {
      return (Float)  ((Byte) d).floatValue();
    }

    public Double toDouble(Object d) {
      return (Double)  ((Byte) d).doubleValue();
    }

    public String toString(Object d) {
      // Converts to the decimal representation of value of byte
      return  ((Byte) d).toString();
    }

    public Object toObject(Object d) {
      return ((Object) d);
    }

    public Byte[] toByteArray(Object d) {
      Byte[] byteArray = new Byte[1];
      byteArray[0] = toByte(d);
      return byteArray;
    }

    public Character[] toCharArray(Object d) {
      Character[] charArray = new Character[1];
      charArray[0] = toCharacter(d);
      return charArray;
    }

    public Short[] toShortArray(Object d) {
      Short[] shortArray = new Short[1];
      shortArray[0] = toShort(d);
      return shortArray;
    }

    public Integer[] toIntegerArray(Object d) {
      Integer[] integerArray = new Integer[1];
      integerArray[0] = toInteger(d);
      return integerArray;
    }

    public Long[] toLongArray(Object d) {
      Long[] longArray = new Long[1];
      longArray[0] = toLong(d);
      return longArray;
    }

    public Float[] toFloatArray(Object d) {
      Float[] floatArray = new Float[1];
      floatArray[0] = toFloat(d);
      return floatArray;
    }

    public Double[] toDoubleArray(Object d) {
      Double[] doubleArray = new Double[1];
      doubleArray[0] = toDouble(d);
      return doubleArray;
    }

    public String[] toStringArray(Object d) {
      String[] stringArray = new String[1];
      stringArray[0] = toString(d);
      return stringArray;
    }

    public Byte convert(Object d, PinotDataType u) {
      return u.toByte(d);
    }
  },

  CHARACTER {
    public Boolean toBoolean(Object d) {
      return null;
    }

    public Byte toByte(Object d) {
      return (Byte) ((byte) ((Character) d).charValue());
    }

    public Character toCharacter(Object d) {
      return (Character) d;
    }

    public Short toShort(Object d) {
      return (Short) ((short) ((Character) d).charValue());
    }

    public Integer toInteger(Object d) {
      return (Integer) ((int) ((Character) d).charValue());
    }

    public Long toLong(Object d) {
      return (Long) ((long) ((Character) d).charValue());
    }

    public Float toFloat(Object d) {
      return (Float) ((float) ((Character) d).charValue());
    }

    public Double toDouble(Object d) {
      return (Double) ((double) ((Character) d).charValue());
    }

    public String toString(Object d) {
      return ((Character) d).toString();
    }

    public Object toObject(Object d) {
      return ((Object) d);
    }

    public Byte[] toByteArray(Object d) {
      Byte[] byteArray = new Byte[1];
      byteArray[0] = toByte(d);
      return byteArray;
    }

    public Character[] toCharArray(Object d) {
      Character[] charArray = new Character[1];
      charArray[0] = ((Character) d);
      return charArray;
    }

    public Short[] toShortArray(Object d) {
      Short[] shortArray = new Short[1];
      shortArray[0] = toShort(d);
      return shortArray;
    }

    public Integer[] toIntegerArray(Object d) {
      Integer[] integerArray = new Integer[1];
      integerArray[0] = toInteger(d);
      return integerArray;
    }

    public Long[] toLongArray(Object d) {
      Long[] longArray = new Long[1];
      longArray[0] = toLong(d);
      return longArray;
    }

    public Float[] toFloatArray(Object d) {
      Float[] floatArray = new Float[1];
      floatArray[0] = toFloat(d);
      return floatArray;
    }

    public Double[] toDoubleArray(Object d) {
      Double[] doubleArray = new Double[1];
      doubleArray[0] = toDouble(d);
      return doubleArray;
    }

    public String[] toStringArray(Object d) {
      String[] stringArray = new String[1];
      stringArray[0] = toString(d);
      return stringArray;
    }

    public Character convert(Object d, PinotDataType u) {
      return u.toCharacter(d);
    }

  },

  SHORT {
    public Boolean toBoolean(Object d) {
      return null;
    }

    public Byte toByte(Object d) {
      return ((Short) d).byteValue();
    }

    public Character toCharacter(Object d) {
      return ((char) ((Short) d).shortValue());
    }

    public Short toShort(Object d) {
      return ((Short) d);
    }

    public Integer toInteger(Object d) {
      return ((Short) d).intValue();
    }

    public Long toLong(Object d) {
      return ((Short) d).longValue();
    }

    public Float toFloat(Object d) {
      return ((Short) d).floatValue();
    }

    public Double toDouble(Object d) {
      return ((Short) d).doubleValue();
    }

    public String toString(Object d) {
      return ((Short) d).toString();
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      Byte[] byteArray = new Byte[1];
      byteArray[0] = toByte(d);
      return byteArray;
    }

    public Character[] toCharArray(Object d) {
      Character[] charArray = new Character[1];
      charArray[0] = toCharacter(d);
      return charArray;
    }

    public Short[] toShortArray(Object d) {
      Short[] shortArray = new Short[1];
      shortArray[0] = toShort(d);
      return shortArray;
    }

    public Integer[] toIntegerArray(Object d) {
      Integer[] integerArray = new Integer[1];
      integerArray[0] = toInteger(d);
      return integerArray;
    }

    public Long[] toLongArray(Object d) {
      Long[] longArray = new Long[1];
      longArray[0] = toLong(d);
      return longArray;
    }

    public Float[] toFloatArray(Object d) {
      Float[] FloatArray = new Float[1];
      FloatArray[0] = toFloat(d);
      return FloatArray;
    }

    public Double[] toDoubleArray(Object d) {
      Double[] doubleArray = new Double[1];
      doubleArray[0] = toDouble(d);
      return doubleArray;
    }

    public String[] toStringArray(Object d) {
      String[] stringArray = new String[1];
      stringArray[0] = toString(d);
      return stringArray;
    }

    public Short convert(Object d, PinotDataType u) {
      return u.toShort(d);
    }

  },
  INTEGER {
    public Boolean toBoolean(Object d) {
      return null;
    }

    public Byte toByte(Object d) {
      return ((Integer) d).byteValue();
    }

    public Character toCharacter(Object d) {
      return (Character) ((char) ((Integer) d).intValue());
    }

    public Short toShort(Object d) {
      return ((Integer) d).shortValue();
    }

    public Integer toInteger(Object d) {
      return ((Integer) d);
    }

    public Long toLong(Object d) {
      return ((Integer) d).longValue();
    }

    public Float toFloat(Object d) {
      return ((Integer) d).floatValue();
    }

    public Double toDouble(Object d) {
      return ((Integer) d).doubleValue();
    }

    public String toString(Object d) {
      return ((Integer) d).toString();
    }

    public Object toObject(Object d) {
      return ((Object) d);
    }

    public Byte[] toByteArray(Object d) {
      Byte[] byteArray = new Byte[1];
      byteArray[0] = toByte(d);
      return byteArray;
    }

    public Character[] toCharArray(Object d) {
      Character[] charArray = new Character[1];
      charArray[0] = toCharacter(d);
      return charArray;
    }

    public Short[] toShortArray(Object d) {
      Short[] shortArray = new Short[1];
      shortArray[0] = toShort(d);
      return shortArray;
    }

    public Integer[] toIntegerArray(Object d) {
      Integer[] integerArray = new Integer[1];
      integerArray[0] = toInteger(d);
      return integerArray;
    }

    public Long[] toLongArray(Object d) {
      Long[] longArray = new Long[1];
      longArray[0] = toLong(d);
      return longArray;
    }

    public Float[] toFloatArray(Object d) {
      Float[] FloatArray = new Float[1];
      FloatArray[0] = toFloat(d);
      return FloatArray;
    }

    public Double[] toDoubleArray(Object d) {
      Double[] doubleArray = new Double[1];
      doubleArray[0] = toDouble(d);
      return doubleArray;
    }

    public String[] toStringArray(Object d) {
      String[] stringArray = new String[1];
      stringArray[0] = toString(d);
      return stringArray;
    }

    public Integer convert(Object d, PinotDataType u) {
      return u.toInteger(d);
    }
  },
  LONG {
    public Boolean toBoolean(Object d) {
      return null;
    }

    public Byte toByte(Object d) {
      return ((Long) d).byteValue();
    }

    public Character toCharacter(Object d) {
      return (char) ((Long) d).longValue();
    }

    public Short toShort(Object d) {
      return ((Long) d).shortValue();
    }

    public Integer toInteger(Object d) {
      return ((Long) d).intValue();
    }

    public Long toLong(Object d) {
      return ((Long) d);
    }

    public Float toFloat(Object d) {
      return ((Long) d).floatValue();
    }

    public Double toDouble(Object d) {
      return ((Long) d).doubleValue();
    }

    public String toString(Object d) {
      return ((Long) d).toString();
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      Byte[] byteArray = new Byte[1];
      byteArray[0] = toByte(d);
      return byteArray;
    }

    public Character[] toCharArray(Object d) {
      Character[] charArray = new Character[1];
      charArray[0] = toCharacter(d);
      return charArray;
    }

    public Short[] toShortArray(Object d) {
      Short[] shortArray = new Short[1];
      shortArray[0] = toShort(d);
      return shortArray;
    }

    public Integer[] toIntegerArray(Object d) {
      Integer[] integerArray = new Integer[1];
      integerArray[0] = toInteger(d);
      return integerArray;
    }

    public Long[] toLongArray(Object d) {
      Long[] longArray = new Long[1];
      longArray[0] = toLong(d);
      return longArray;
    }

    public Float[] toFloatArray(Object d) {
      Float[] FloatArray = new Float[1];
      FloatArray[0] = toFloat(d);
      return FloatArray;
    }

    public Double[] toDoubleArray(Object d) {
      Double[] doubleArray = new Double[1];
      doubleArray[0] = toDouble(d);
      return doubleArray;
    }

    public String[] toStringArray(Object d) {
      String[] stringArray = new String[1];
      stringArray[0] = toString(d);
      return stringArray;
    }

    public Long convert(Object d, PinotDataType u) {
      return u.toLong(d);
    }

  },

  FLOAT {
    public Boolean toBoolean(Object d) {
      return null;
    }

    public Byte toByte(Object d) {
      return ((Float) d).byteValue();
    }

    public Character toCharacter(Object d) {
      return (char) ((Float) d).floatValue();
    }

    public Short toShort(Object d) {
      return ((Float) d).shortValue();
    }

    public Integer toInteger(Object d) {
      return ((Float) d).intValue();
    }

    public Long toLong(Object d) {
      return ((Float) d).longValue();
    }

    public Float toFloat(Object d) {
      return ((Float) d);
    }

    public Double toDouble(Object d) {
      return ((Float) d).doubleValue();
    }

    public String toString(Object d) {
      return ((Float) d).toString();
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      Byte[] byteArray = new Byte[1];
      byteArray[0] = toByte(d);
      return byteArray;
    }

    public Character[] toCharArray(Object d) {
      Character[] charArray = new Character[1];
      charArray[0] = toCharacter(d);
      return charArray;
    }

    public Short[] toShortArray(Object d) {
      Short[] shortArray = new Short[1];
      shortArray[0] = toShort(d);
      return shortArray;
    }

    public Integer[] toIntegerArray(Object d) {
      Integer[] integerArray = new Integer[1];
      integerArray[0] = toInteger(d);
      return integerArray;
    }

    public Long[] toLongArray(Object d) {
      Long[] longArray = new Long[1];
      longArray[0] = toLong(d);
      return longArray;
    }

    public Float[] toFloatArray(Object d) {
      Float[] FloatArray = new Float[1];
      FloatArray[0] = toFloat(d);
      return FloatArray;
    }

    public Double[] toDoubleArray(Object d) {
      Double[] doubleArray = new Double[1];
      doubleArray[0] = toDouble(d);
      return doubleArray;
    }

    public String[] toStringArray(Object d) {
      String[] stringArray = new String[1];
      stringArray[0] = toString(d);
      return stringArray;
    }

    public Float convert(Object d, PinotDataType u) {
      return u.toFloat(d);
    }
  },

  DOUBLE {
    public Boolean toBoolean(Object d) {
      return null;
    }

    public Byte toByte(Object d) {
      return ((Double) d).byteValue();
    }

    public Character toCharacter(Object d) {
      return (char) ((Double) d).doubleValue();
    }

    public Short toShort(Object d) {
      return ((Double) d).shortValue();
    }

    public Integer toInteger(Object d) {
      return ((Double) d).intValue();
    }

    public Long toLong(Object d) {
      return ((Double) d).longValue();
    }

    public Float toFloat(Object d) {
      return ((Double) d).floatValue();
    }

    public Double toDouble(Object d) {
      return ((Double) d).doubleValue();
    }

    public String toString(Object d) {
      return ((Double) d).toString();
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      Byte[] byteArray = new Byte[1];
      byteArray[0] = toByte(d);
      return byteArray;
    }

    public Character[] toCharArray(Object d) {
      Character[] charArray = new Character[1];
      charArray[0] = toCharacter(d);
      return charArray;
    }

    public Short[] toShortArray(Object d) {
      Short[] shortArray = new Short[1];
      shortArray[0] = toShort(d);
      return shortArray;
    }

    public Integer[] toIntegerArray(Object d) {
      Integer[] integerArray = new Integer[1];
      integerArray[0] = toInteger(d);
      return integerArray;
    }

    public Long[] toLongArray(Object d) {
      Long[] longArray = new Long[1];
      longArray[0] = toLong(d);
      return longArray;
    }

    public Float[] toFloatArray(Object d) {
      Float[] FloatArray = new Float[1];
      FloatArray[0] = toFloat(d);
      return FloatArray;
    }

    public Double[] toDoubleArray(Object d) {
      Double[] doubleArray = new Double[1];
      doubleArray[0] = toDouble(d);
      return doubleArray;
    }

    public String[] toStringArray(Object d) {
      String[] stringArray = new String[1];
      stringArray[0] = toString(d);
      return stringArray;
    }

    public Double convert(Object d, PinotDataType u) {
      return u.toDouble(d);
    }
  },
  STRING {
    public Boolean toBoolean(Object d) {
      return Boolean.parseBoolean((String) d);
    }

    public Byte toByte(Object d) {
      return (byte) (((String) d).charAt(0));
    }

    public Character toCharacter(Object d) {
      return ((String) d).charAt(0);
    }

    public Short toShort(Object d) {
      short s;
      try {
        s = Short.parseShort(((String) d));
      } catch (NumberFormatException e) {
        return null;
      }
      return s;
    }

    public Integer toInteger(Object d) {
      int s;
      try {
        s = Integer.parseInt(((String) d));
      } catch (NumberFormatException e) {
        return null;
      }
      return s;
    }

    public Long toLong(Object d) {
      long s;
      try {
        s = Long.parseLong(((String) d));
      } catch (NumberFormatException e) {
        return null;
      }
      return s;
    }

    public Float toFloat(Object d) {
      float s;
      try {
        s = Float.parseFloat(((String) d));
      } catch (NumberFormatException e) {
        return null;
      }
      return s;
    }

    public Double toDouble(Object d) {
      double s;
      try {
        s = Double.parseDouble(((String) d));
      } catch (NumberFormatException e) {
        return null;
      }
      return s;
    }

    public String toString(Object d) {
      return ((String) d);
    }

    public Object toObject(Object d) {
      return ((Object) d);
    }

    public Byte[] toByteArray(Object d) {
      Byte[] byteArray = new Byte[1];
      byteArray[0] = toByte(d);
      return byteArray;
    }

    public Character[] toCharArray(Object d) {
      Character[] charArray = new Character[1];
      charArray[0] = toCharacter(d);
      return charArray;
    }

    public Short[] toShortArray(Object d) {
      Short[] shortArray = new Short[1];
      shortArray[0] = toShort(d);
      return shortArray;
    }

    public Integer[] toIntegerArray(Object d) {
      Integer[] integerArray = new Integer[1];
      integerArray[0] = toInteger(d);
      return integerArray;
    }

    public Long[] toLongArray(Object d) {
      Long[] longArray = new Long[1];
      longArray[0] = toLong(d);
      return longArray;
    }

    public Float[] toFloatArray(Object d) {
      Float[] FloatArray = new Float[1];
      FloatArray[0] = toFloat(d);
      return FloatArray;
    }

    public Double[] toDoubleArray(Object d) {
      Double[] doubleArray = new Double[1];
      doubleArray[0] = toDouble(d);
      return doubleArray;
    }

    public String[] toStringArray(Object d) {
      String[] stringArray = new String[1];
      stringArray[0] = toString(d);
      return stringArray;
    }

    public String convert(Object d, PinotDataType u) {
      return u.toString(d);
    }

  },

  OBJECT {
    public Boolean toBoolean(Object d) {
      return (Boolean) d;
    }

    public Byte toByte(Object d) {
      return (Byte) d;
    }

    public Character toCharacter(Object d) {
      return (Character) d;
    }

    public Short toShort(Object d) {
      if (d instanceof Number) {
        d = ((Number) d).shortValue();
      }
      if (d instanceof Object[]) {
        d = toShort(((Object[]) d)[0]);
      }
      return (Short) d;
    }

    public Integer toInteger(Object d) {
      if (d instanceof Number) {
        d = ((Number) d).intValue();
      }
      if (d instanceof Object[]) {
        d = toInteger(((Object[]) d)[0]);
      }
      return (Integer) d;
    }

    public Long toLong(Object d) {
      if (d instanceof Number) {
        d = ((Number) d).longValue();
      }
      if (d instanceof Object[]) {
        d = toLong(((Object[]) d)[0]);
      }
      return (Long) d;
    }

    public Float toFloat(Object d) {
      if (d instanceof Number) {
        d = ((Number) d).floatValue();
      }
      if (d instanceof Object[]) {
        d = toFloat(((Object[]) d)[0]);
      }
      return (Float) d;
    }

    public Double toDouble(Object d) {
      if (d instanceof Number) {
        d = ((Number) d).doubleValue();
      }
      if (d instanceof Object[]) {
        d = toDouble(((Object[]) d)[0]);
      }
      return (Double) d;
    }

    public String toString(Object d) {
      return (String) ((Object) d);
    }

    public Object toObject(Object d) {
      return ((Object) d);
    }

    public Byte[] toByteArray(Object d) {
      return (Byte[]) ((Object) d);
    }

    public Character[] toCharArray(Object d) {
      return (Character[]) ((Object) d);
    }

    public Short[] toShortArray(Object d) {
      return (Short[]) ((Object) d);
    }

    public Integer[] toIntegerArray(Object d) {
      return (Integer[]) ((Object) d);
    }

    public Long[] toLongArray(Object d) {
      return (Long[]) ((Object) d);
    }

    public Float[] toFloatArray(Object d) {
      return (Float[]) ((Object) d);
    }

    public Double[] toDoubleArray(Object d) {
      return (Double[]) ((Object) d);
    }

    public String[] toStringArray(Object d) {
      return (String[]) ((Object) d);
    }

    public Object convert(Object d, PinotDataType u) {
      return d;
    }
  },

  BYTE_ARRAY {
    public Boolean toBoolean(Object d) {
      return null;
    }

    public Byte toByte(Object d) {
      return ((Byte[]) d)[0];
    }

    public Character toCharacter(Object d) {
      return BYTE.toCharacter(((Byte[]) d)[0]);
    }

    public Short toShort(Object d) {
      return BYTE.toShort(((Byte[])d)[0]);
    }

    public Integer toInteger(Object d) {
      return BYTE.toInteger(((Byte[])d)[0]);
    }

    public Long toLong(Object d) {
      return BYTE.toLong(((Byte[])d)[0]);
    }

    public Float toFloat(Object d) {
      return BYTE.toFloat(((Byte[])d)[0]);
    }

    public Double toDouble(Object d) {
      return BYTE.toDouble(((Byte[])d)[0]);
    }

    public String toString(Object d) {
      return BYTE.toString(((Byte[])d)[0]);
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      return (Byte[]) d;
    }

    public Character[] toCharArray(Object d) {
      Character[] charArray = new Character[((Byte[]) d).length];
      for (int i = 0; i < ((Byte[]) d).length; i++) {
        charArray[i] = BYTE.toCharacter(((Byte[]) d)[i]);
      }
      return charArray;
    }

    public Short[] toShortArray(Object d) {
      Short[] shortArray = new Short[((Byte[]) d).length];
      for (int i = 0; i < ((Byte[]) d).length; i++) {
        shortArray[i] = BYTE.toShort(((Byte[]) d)[i]);
      }
      return shortArray;
    }

    public Integer[] toIntegerArray(Object d) {
      Integer[] intArray = new Integer[((Byte[]) d).length];
      for (int i = 0; i < ((Byte[]) d).length; i++) {
        intArray[i] = BYTE.toInteger(((Byte[]) d)[i]);
      }
      return intArray;
    }

    public Long[] toLongArray(Object d) {
      Long[] longArray = new Long[((Byte[]) d).length];
      for (int i = 0; i < ((Byte[]) d).length; i++) {
        longArray[i] = BYTE.toLong(((Byte[]) d)[i]);
      }
      return longArray;
    }

    public Float[] toFloatArray(Object d) {
      Float[] floatArray = new Float[((Byte[]) d).length];
      for (int i = 0; i < ((Byte[]) d).length; i++) {
        floatArray[i] = BYTE.toFloat(((Byte[]) d)[i]);
      }
      return floatArray;
    }

    public Double[] toDoubleArray(Object d) {
      return (Double[]) d;
    }

    public String[] toStringArray(Object d) {
      String[] stringArray = new String[((Byte[]) d).length];
      for (int i = 0; i < ((Byte[]) d).length; i++) {
        stringArray[i] = BYTE.toString(((Byte[]) d)[i]);
      }
      return stringArray;
    }

    public Byte[] convert(Object d, PinotDataType u) {
      return u.toByteArray(d);
    }
  },
  CHARACTER_ARRAY {
    public Boolean toBoolean(Object d) {
      return null;
    }

    public Byte toByte(Object d) {
      return CHARACTER.toByte(((Character[]) d)[0]);
    }

    public Character toCharacter(Object d) {
      return ((Character[]) d)[0];
    }

    public Short toShort(Object d) {
      return CHARACTER.toShort(((Character[]) d)[0]);
    }

    public Integer toInteger(Object d) {
      return CHARACTER.toInteger(((Character[]) d)[0]);
    }

    public Long toLong(Object d) {
      return CHARACTER.toLong(((Character[]) d)[0]);
    }

    public Float toFloat(Object d) {
      return CHARACTER.toFloat(((Character[]) d)[0]);
    }

    public Double toDouble(Object d) {
      return CHARACTER.toDouble(((Character[]) d)[0]);
    }

    public String toString(Object d) {
      return CHARACTER.toString(((Character[]) d)[0]);
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      Byte[] byteArray = new Byte[((Character[]) d).length];
      for (int i = 0; i < ((Character[]) d).length; i++) {
        byteArray[i] = CHARACTER.toByte(((Character[]) d)[i]);
      }
      return byteArray;
    }

    public Character[] toCharArray(Object d) {
      return (Character[]) d;
    }

    public Short[] toShortArray(Object d) {
      Short[] shortArray = new Short[((Character[]) d).length];
      for (int i = 0; i < ((Character[]) d).length; i++) {
        shortArray[i] = CHARACTER.toShort(((Character[]) d)[i]);
      }
      return shortArray;
    }

    public Integer[] toIntegerArray(Object d) {
      Integer[] integerArray = new Integer[((Character[]) d).length];
      for (int i = 0; i < ((Character[]) d).length; i++) {
        integerArray[i] = CHARACTER.toInteger(((Character[]) d)[i]);
      }
      return integerArray;
    }

    public Long[] toLongArray(Object d) {
      Long[] longArray = new Long[((Character[]) d).length];
      for (int i = 0; i < ((Character[]) d).length; i++) {
        longArray[i] = CHARACTER.toLong(((Character[]) d)[i]);
      }
      return longArray;
    }

    public Float[] toFloatArray(Object d) {
      Float[] floatArray = new Float[((Character[]) d).length];
      for (int i = 0; i < ((Character[]) d).length; i++) {
        floatArray[i] = CHARACTER.toFloat(((Character[]) d)[i]);
      }
      return floatArray;
    }

    public Double[] toDoubleArray(Object d) {
      Double[] doubleArray = new Double[((Character[]) d).length];
      for (int i = 0; i < ((Character[]) d).length; i++) {
        doubleArray[i] = CHARACTER.toDouble(((Character[]) d)[i]);
      }
      return doubleArray;
    }

    public String[] toStringArray(Object d) {
      String[] stringArray = new String[((Character[]) d).length];
      for (int i = 0; i < ((Character[]) d).length; i++) {
        stringArray[i] = CHARACTER.toString(((Character[]) d)[i]);
      }
      return stringArray;
    }

    public Character[] convert(Object d, PinotDataType u) {
      return u.toCharArray(d);
    }
  },
  SHORT_ARRAY {
    public Boolean toBoolean(Object d) {
      return null;
    }

    public Byte toByte(Object d) {
      return SHORT.toByte(((Short[]) d)[0]);
    }

    public Character toCharacter(Object d) {
      return SHORT.toCharacter(((Short[]) d)[0]);
    }

    public Short toShort(Object d) {
      return ((Short[]) d)[0];
    }

    public Integer toInteger(Object d) {
      return SHORT.toInteger(((Short[]) d)[0]);
    }

    public Long toLong(Object d) {
      return SHORT.toLong(((Short[]) d)[0]);
    }

    public Float toFloat(Object d) {
      return SHORT.toFloat(((Short[]) d)[0]);
    }

    public Double toDouble(Object d) {
      return SHORT.toDouble(((Short[]) d)[0]);
    }

    public String toString(Object d) {
      return SHORT.toString(((Short[]) d)[0]);
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      Byte[] byteArray = new Byte[((Short[]) d).length];
      for (int i = 0; i < ((Short[]) d).length; i++) {
        byteArray[i] = SHORT.toByte(((Short[]) d)[i]);
      }
      return byteArray;
    }

    public Character[] toCharArray(Object d) {
      Character[] charArray = new Character[((Short[]) d).length];
      for (int i = 0; i < ((Short[]) d).length; i++) {
        charArray[i] = SHORT.toCharacter(((Short[]) d)[i]);
      }
      return charArray;
    }

    public Short[] toShortArray(Object d) {
      return (Short[]) d;
    }

    public Integer[] toIntegerArray(Object d) {
      Integer[] intArray = new Integer[((Short[]) d).length];
      for (int i = 0; i < ((Short[]) d).length; i++) {
        intArray[i] = SHORT.toInteger(((Short[]) d)[i]);
      }
      return intArray;
    }

    public Long[] toLongArray(Object d) {
      Long[] longArray = new Long[((Short[]) d).length];
      for (int i = 0; i < ((Short[]) d).length; i++) {
        longArray[i] = SHORT.toLong(((Short[]) d)[i]);
      }
      return longArray;
    }

    public Float[] toFloatArray(Object d) {
      Float[] floatArray = new Float[((Short[]) d).length];
      for (int i = 0; i < ((Short[]) d).length; i++) {
        floatArray[i] = SHORT.toFloat(((Short[]) d)[i]);
      }
      return floatArray;
    }

    public Double[] toDoubleArray(Object d) {
      Double[] doubleArray = new Double[((Short[]) d).length];
      for (int i = 0; i < ((Short[]) d).length; i++) {
        doubleArray[i] = SHORT.toDouble(((Short[]) d)[i]);
      }
      return doubleArray;
    }

    public String[] toStringArray(Object d) {
      String[] stringArray = new String[((Short[]) d).length];
      for (int i = 0; i < ((Short[]) d).length; i++) {
        stringArray[i] = SHORT.toString(((Short[]) d)[i]);
      }
      return stringArray;
    }

    public Short[] convert(Object d, PinotDataType u) {
      return u.toShortArray(d);
    }
  },
  INTEGER_ARRAY {
    public Boolean toBoolean(Object d) {
      return null;
    }

    public Byte toByte(Object d) {
      return INTEGER.toByte(((Integer[]) d)[0]);
    }

    public Character toCharacter(Object d) {
      return INTEGER.toCharacter(((Integer[]) d)[0]);
    }

    public Short toShort(Object d) {
      return INTEGER.toShort(((Integer[]) d)[0]);
    }

    public Integer toInteger(Object d) {
      return ((Integer[]) d)[0];
    }

    public Long toLong(Object d) {
      return INTEGER.toLong(((Integer[]) d)[0]);
    }

    public Float toFloat(Object d) {
      return INTEGER.toFloat(((Integer[]) d)[0]);
    }

    public Double toDouble(Object d) {
      return INTEGER.toDouble(((Integer[]) d)[0]);
    }

    public String toString(Object d) {
      return INTEGER.toString(((Integer[]) d)[0]);
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      Byte[] byteArray = new Byte[((Integer[]) d).length];
      for (int i = 0; i < ((Integer[]) d).length; i++) {
        byteArray[i] = INTEGER.toByte(((Integer[]) d)[i]);
      }
      return byteArray;
    }

    public Character[] toCharArray(Object d) {
      Character[] charArray = new Character[((Integer[]) d).length];
      for (int i = 0; i < ((Integer[]) d).length; i++) {
        charArray[i] = INTEGER.toCharacter(((Integer[]) d)[i]);
      }
      return charArray;
    }

    public Short[] toShortArray(Object d) {
      Short[] shortArray = new Short[((Integer[]) d).length];
      for (int i = 0; i < ((Integer[]) d).length; i++) {
        shortArray[i] = INTEGER.toShort(((Integer[]) d)[i]);
      }
      return shortArray;
    }

    public Integer[] toIntegerArray(Object d) {
      return (Integer[]) d;
    }

    public Long[] toLongArray(Object d) {
      Long[] longArray = new Long[((Integer[]) d).length];
      for (int i = 0; i < ((Integer[]) d).length; i++) {
        longArray[i] = INTEGER.toLong(((Integer[]) d)[i]);
      }
      return longArray;
    }

    public Float[] toFloatArray(Object d) {
      Float[] floatArray = new Float[((Integer[]) d).length];
      for (int i = 0; i < ((Integer[]) d).length; i++) {
        floatArray[i] = INTEGER.toFloat(((Integer[]) d)[i]);
      }
      return floatArray;
    }

    public Double[] toDoubleArray(Object d) {
      Double[] doubleArray = new Double[((Integer[]) d).length];
      for (int i = 0; i < ((Integer[]) d).length; i++) {
        doubleArray[i] = INTEGER.toDouble(((Integer[]) d)[i]);
      }
      return doubleArray;
    }

    public String[] toStringArray(Object d) {
      String[] stringArray = new String[((Integer[]) d).length];
      for (int i = 0; i < ((Integer[]) d).length; i++) {
        stringArray[i] = INTEGER.toString(((Integer[]) d)[i]);
      }
      return stringArray;
    }

    public Integer[] convert(Object d, PinotDataType u) {
      return u.toIntegerArray(d);
    }
  },
  LONG_ARRAY {
    public Boolean toBoolean(Object d) {
      return null;
    }

    public Byte toByte(Object d) {
      return LONG.toByte(((Long[]) d)[0]);
    }

    public Character toCharacter(Object d) {
      return LONG.toCharacter(((Long[]) d)[0]);
    }

    public Short toShort(Object d) {
      return LONG.toShort(((Long[]) d)[0]);
    }

    public Integer toInteger(Object d) {
      return LONG.toInteger(((Long[]) d)[0]);
    }

    public Long toLong(Object d) {
      return ((Long[]) d)[0];
    }

    public Float toFloat(Object d) {
      return LONG.toFloat(((Long[]) d)[0]);
    }

    public Double toDouble(Object d) {
      return LONG.toDouble(((Long[]) d)[0]);
    }

    public String toString(Object d) {
      return LONG.toString(((Long[]) d)[0]);
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      Byte[] byteArray = new Byte[((Long[]) d).length];
      for (int i = 0; i < ((Long[]) d).length; i++) {
        byteArray[i] = LONG.toByte(((Long[]) d)[i]);
      }
      return byteArray;
    }

    public Character[] toCharArray(Object d) {
      Character[] byteArray = new Character[((Long[]) d).length];
      for (int i = 0; i < ((Long[]) d).length; i++) {
        byteArray[i] = LONG.toCharacter(((Long[]) d)[i]);
      }
      return byteArray;
    }

    public Short[] toShortArray(Object d) {
      Short[] shortArray = new Short[((Long[]) d).length];
      for (int i = 0; i < ((Long[]) d).length; i++) {
        shortArray[i] = LONG.toShort(((Long[]) d)[i]);
      }
      return shortArray;
    }

    public Integer[] toIntegerArray(Object d) {
      Integer[] intArray = new Integer[((Long[]) d).length];
      for (int i = 0; i < ((Long[]) d).length; i++) {
        intArray[i] = LONG.toInteger(((Long[]) d)[i]);
      }
      return intArray;
    }

    public Long[] toLongArray(Object d) {
      return (Long[]) d;
    }

    public Float[] toFloatArray(Object d) {
      Float[] floatArray = new Float[((Long[]) d).length];
      for (int i = 0; i < ((Long[]) d).length; i++) {
        floatArray[i] = LONG.toFloat(((Long[]) d)[i]);
      }
      return floatArray;
    }

    public Double[] toDoubleArray(Object d) {
      Double[] doubleArray = new Double[((Long[]) d).length];
      for (int i = 0; i < ((Long[]) d).length; i++) {
        doubleArray[i] = LONG.toDouble(((Long[]) d)[i]);
      }
      return doubleArray;
    }

    public String[] toStringArray(Object d) {
      String[] stringArray = new String[((Long[]) d).length];
      for (int i = 0; i < ((Long[]) d).length; i++) {
        stringArray[i] = LONG.toString(((Long[]) d)[i]);
      }
      return stringArray;
    }

    public Long[] convert(Object d, PinotDataType u) {
      return u.toLongArray(d);
    }
  },
  FLOAT_ARRAY {
    public Boolean toBoolean(Object d) {
      return null;
    }

    public Byte toByte(Object d) {
      return FLOAT.toByte(((Float[]) d)[0]);
    }

    public Character toCharacter(Object d) {
      return FLOAT.toCharacter(((Float[]) d)[0]);
    }

    public Short toShort(Object d) {
      return FLOAT.toShort(((Float[]) d)[0]);
    }

    public Integer toInteger(Object d) {
      return FLOAT.toInteger(((Float[]) d)[0]);
    }

    public Long toLong(Object d) {
      return FLOAT.toLong(((Float[]) d)[0]);
    }

    public Float toFloat(Object d) {
      return ((Float[]) d)[0];
    }

    public Double toDouble(Object d) {
      return FLOAT.toDouble(((Float[]) d)[0]);
    }

    public String toString(Object d) {
      return FLOAT.toString(((Float[]) d)[0]);
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      Byte[] byteArray = new Byte[((Float[]) d).length];
      for (int i = 0; i < ((Float[]) d).length; i++) {
        byteArray[i] = FLOAT.toByte(((Float[]) d)[i]);
      }
      return byteArray;
    }

    public Character[] toCharArray(Object d) {
      Character[] charArray = new Character[((Float[]) d).length];
      for (int i = 0; i < ((Float[]) d).length; i++) {
        charArray[i] = FLOAT.toCharacter(((Float[]) d)[i]);
      }
      return charArray;
    }

    public Short[] toShortArray(Object d) {
      Short[] shortArray = new Short[((Float[]) d).length];
      for (int i = 0; i < ((Float[]) d).length; i++) {
        shortArray[i] = FLOAT.toShort(((Float[]) d)[i]);
      }
      return shortArray;
    }

    public Integer[] toIntegerArray(Object d) {
      Integer[] intArray = new Integer[((Float[]) d).length];
      for (int i = 0; i < ((Float[]) d).length; i++) {
        intArray[i] = FLOAT.toInteger(((Float[]) d)[i]);
      }
      return intArray;
    }

    public Long[] toLongArray(Object d) {
      Long[] longArray = new Long[((Float[]) d).length];
      for (int i = 0; i < ((Float[]) d).length; i++) {
        longArray[i] = FLOAT.toLong(((Float[]) d)[i]);
      }
      return longArray;
    }

    public Float[] toFloatArray(Object d) {
      return (Float[]) d;
    }

    public Double[] toDoubleArray(Object d) {
      Double[] doubleArray = new Double[((Float[]) d).length];
      for (int i = 0; i < ((Float[]) d).length; i++) {
        doubleArray[i] = FLOAT.toDouble(((Float[]) d)[i]);
      }
      return doubleArray;
    }

    public String[] toStringArray(Object d) {
      String[] stringArray = new String[((Float[]) d).length];
      for (int i = 0; i < ((Float[]) d).length; i++) {
        stringArray[i] = FLOAT.toString(((Float[]) d)[i]);
      }
      return stringArray;
    }

    public Float[] convert(Object d, PinotDataType u) {
      return u.toFloatArray(d);
    }
  },
  DOUBLE_ARRAY {
    public Boolean toBoolean(Object d) {
      return null;
    }

    public Byte toByte(Object d) {
      return DOUBLE.toByte(((Double[]) d)[0]);
    }

    public Character toCharacter(Object d) {
      return DOUBLE.toCharacter(((Double[]) d)[0]);
    }

    public Short toShort(Object d) {
      return DOUBLE.toShort(((Double[]) d)[0]);
    }

    public Integer toInteger(Object d) {
      return DOUBLE.toInteger(((Double[]) d)[0]);
    }

    public Long toLong(Object d) {
      return DOUBLE.toLong(((Double[]) d)[0]);
    }

    public Float toFloat(Object d) {
      return DOUBLE.toFloat(((Double[]) d)[0]);
    }

    public Double toDouble(Object d) {
      return ((Double[]) d)[0];
    }

    public String toString(Object d) {
      return DOUBLE.toString(((Double[]) d)[0]);
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      Byte[] byteArray = new Byte[((Double[]) d).length];
      for (int i = 0; i < ((Double[]) d).length; i++) {
        byteArray[i] = DOUBLE.toByte(((Double[]) d)[i]);
      }
      return byteArray;
    }

    public Character[] toCharArray(Object d) {
      Character[] charArray = new Character[((Double[]) d).length];
      for (int i = 0; i < ((Double[]) d).length; i++) {
        charArray[i] = DOUBLE.toCharacter(((Double[]) d)[i]);
      }
      return charArray;
    }

    public Short[] toShortArray(Object d) {
      Short[] shortArray = new Short[((Double[]) d).length];
      for (int i = 0; i < ((Double[]) d).length; i++) {
        shortArray[i] = DOUBLE.toShort(((Double[]) d)[i]);
      }
      return shortArray;
    }

    public Integer[] toIntegerArray(Object d) {
      Integer[] intArray = new Integer[((Double[]) d).length];
      for (int i = 0; i < ((Double[]) d).length; i++) {
        intArray[i] = DOUBLE.toInteger(((Double[]) d)[i]);
      }
      return intArray;
    }

    public Long[] toLongArray(Object d) {
      Long[] longArray = new Long[((Double[]) d).length];
      for (int i = 0; i < ((Double[]) d).length; i++) {
        longArray[i] = DOUBLE.toLong(((Double[]) d)[i]);
      }
      return longArray;
    }

    public Float[] toFloatArray(Object d) {
      Float[] floatArray = new Float[((Double[]) d).length];
      for (int i = 0; i < ((Double[]) d).length; i++) {
        floatArray[i] = DOUBLE.toFloat(((Double[]) d)[i]);
      }
      return floatArray;
    }

    public Double[] toDoubleArray(Object d) {
      return (Double[]) d;
    }

    public String[] toStringArray(Object d) {
      String[] stringArray = new String[((Double[]) d).length];
      for (int i = 0; i < ((Double[]) d).length; i++) {
        stringArray[i] = DOUBLE.toString(((Double[]) d)[i]);
      }
      return stringArray;
    }

    public Double[] convert(Object d, PinotDataType u) {
      return u.toDoubleArray(d);
    }
  },
  STRING_ARRAY {
    public Boolean toBoolean(Object d) {
      return null;
    }

    public Byte toByte(Object d) {
      return STRING.toByte(((String[]) d)[0]);
    }

    public Character toCharacter(Object d) {
      return STRING.toCharacter(((String[]) d)[0]);
    }

    public Short toShort(Object d) {
      return STRING.toShort(((String[]) d)[0]);
    }

    public Integer toInteger(Object d) {
      return STRING.toInteger(((String[]) d)[0]);
    }

    public Long toLong(Object d) {
      return STRING.toLong(((String[]) d)[0]);
    }

    public Float toFloat(Object d) {
      return STRING.toFloat(((String[]) d)[0]);
    }

    public Double toDouble(Object d) {
      return STRING.toDouble(((String[]) d)[0]);
    }

    public String toString(Object d) {
      return ((String[]) d)[0];
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      Byte[] byteArray = new Byte[((String[]) d).length];
      for (int i = 0; i < ((String[]) d).length; i++) {
        byteArray[i] = STRING.toByte(((String[]) d)[i]);
      }
      return byteArray;
    }

    public Character[] toCharArray(Object d) {
      Character[] charArray = new Character[((String[]) d).length];
      for (int i = 0; i < ((String[]) d).length; i++) {
        charArray[i] = STRING.toCharacter(((String[]) d)[i]);
      }
      return charArray;
    }

    public Short[] toShortArray(Object d) {
      Short[] shortArray = new Short[((String[]) d).length];
      for (int i = 0; i < ((String[]) d).length; i++) {
        shortArray[i] = STRING.toShort(((String[]) d)[i]);
      }
      return shortArray;
    }

    public Integer[] toIntegerArray(Object d) {
      Integer[] intArray = new Integer[((String[]) d).length];
      for (int i = 0; i < ((String[]) d).length; i++) {
        intArray[i] = STRING.toInteger(((String[]) d)[i]);
      }
      return intArray;
    }

    public Long[] toLongArray(Object d) {
      Long[] longArray = new Long[((String[]) d).length];
      for (int i = 0; i < ((String[]) d).length; i++) {
        longArray[i] = STRING.toLong(((String[]) d)[i]);
      }
      return longArray;
    }

    public Float[] toFloatArray(Object d) {
      Float[] floatArray = new Float[((String[]) d).length];
      for (int i = 0; i < ((String[]) d).length; i++) {
        floatArray[i] = STRING.toFloat(((String[]) d)[i]);
      }
      return floatArray;
    }

    public Double[] toDoubleArray(Object d) {
      Double[] doubleArray = new Double[((String[]) d).length];
      for (int i = 0; i < ((String[]) d).length; i++) {
        doubleArray[i] = STRING.toDouble(((String[]) d)[i]);
      }
      return doubleArray;
    }

    public String[] toStringArray(Object d) {
      return (String[]) d;
    }

    public String[] convert(Object d, PinotDataType u) {
      return u.toStringArray(d);
    }
  };

  abstract public Boolean toBoolean(Object d);

  abstract public Byte toByte(Object d);

  abstract public Character toCharacter(Object d);

  abstract public Short toShort(Object d);

  abstract public Integer toInteger(Object d);

  abstract public Long toLong(Object d);

  abstract public Float toFloat(Object d);

  abstract public Double toDouble(Object d);

  abstract public String toString(Object d);

  abstract public Object toObject(Object d);

  abstract public Byte[] toByteArray(Object d);

  abstract public Character[] toCharArray(Object d);

  abstract public Short[] toShortArray(Object d);

  abstract public Integer[] toIntegerArray(Object d);

  abstract public Long[] toLongArray(Object d);

  abstract public Float[] toFloatArray(Object d);

  abstract public Double[] toDoubleArray(Object d);

  abstract public String[] toStringArray(Object d);


  public static PinotDataType getPinotDataType(FieldSpec fieldSpec) {
    PinotDataType retval = OBJECT;
    switch (fieldSpec.getDataType()) {
      case BOOLEAN:
        retval = PinotDataType.BOOLEAN;
        break;
      case BYTE:
        retval = fieldSpec.isSingleValueField() ? PinotDataType.BYTE : PinotDataType.BYTE_ARRAY;
        break;
      case CHAR:
        retval = fieldSpec.isSingleValueField() ? PinotDataType.CHARACTER : PinotDataType.CHARACTER_ARRAY;
        break;
      case SHORT:
        retval = fieldSpec.isSingleValueField() ? PinotDataType.SHORT : PinotDataType.SHORT_ARRAY;
        break;
      case INT:
        retval = fieldSpec.isSingleValueField() ? PinotDataType.INTEGER : PinotDataType.INTEGER_ARRAY;
        break;
      case LONG:
        retval = fieldSpec.isSingleValueField() ? PinotDataType.LONG : PinotDataType.LONG_ARRAY;
        break;
      case FLOAT:
        retval = fieldSpec.isSingleValueField() ? PinotDataType.FLOAT : PinotDataType.FLOAT_ARRAY;
        break;
      case DOUBLE:
        retval = fieldSpec.isSingleValueField() ? PinotDataType.DOUBLE : PinotDataType.DOUBLE_ARRAY;
        break;
      case STRING:
        retval = fieldSpec.isSingleValueField() ? PinotDataType.STRING : PinotDataType.STRING_ARRAY;
        break;
      default:
        break;
    }
    return retval;
  }

  abstract public Object convert(Object sourceValue, PinotDataType sourceUnit);
}
