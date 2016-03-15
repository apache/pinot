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
import org.apache.zookeeper.KeeperException;


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
      return null;
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
      return null;
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
      return (Byte) d;
    }

    public Character toCharacter(Object d) {
      return (Character) d;
    }

    public Short toShort(Object d) {
      return (Short) d;
    }

    public Integer toInteger(Object d) {
      return (Integer) d;
    }

    public Long toLong(Object d) {
      return (Long) d;
    }

    public Float toFloat(Object d) {
      return (Float) d;
    }

    public Double toDouble(Object d) {
      return (Double) d;
    }

    public String toString(Object d) {
      return (String) d;
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      return (Byte[]) d;
    }

    public Character[] toCharArray(Object d) {
      return (Character[]) d;
    }

    public Short[] toShortArray(Object d) {
      return (Short[]) d;
    }

    public Integer[] toIntegerArray(Object d) {
      return (Integer[]) d;
    }

    public Long[] toLongArray(Object d) {
      return (Long[]) d;
    }

    public Float[] toFloatArray(Object d) {
      return (Float[]) d;
    }

    public Double[] toDoubleArray(Object d) {
      return (Double[]) d;
    }

    public String[] toStringArray(Object d) {
      return (String[]) d;
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
      return (Byte) d;
    }

    public Character toCharacter(Object d) {
      return (Character) d;
    }

    public Short toShort(Object d) {
      return (Short) d;
    }

    public Integer toInteger(Object d) {
      return (Integer) d;
    }

    public Long toLong(Object d) {
      return (Long) d;
    }

    public Float toFloat(Object d) {
      return (Float) d;
    }

    public Double toDouble(Object d) {
      return (Double) d;
    }

    public String toString(Object d) {
      return (String) d;
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      return (Byte[]) d;
    }

    public Character[] toCharArray(Object d) {
      return (Character[]) d;
    }

    public Short[] toShortArray(Object d) {
      return (Short[]) d;
    }

    public Integer[] toIntegerArray(Object d) {
      return (Integer[]) d;
    }

    public Long[] toLongArray(Object d) {
      return (Long[]) d;
    }

    public Float[] toFloatArray(Object d) {
      return (Float[]) d;
    }

    public Double[] toDoubleArray(Object d) {
      return (Double[]) d;
    }

    public String[] toStringArray(Object d) {
      return (String[]) d;
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
      return (Byte) d;
    }

    public Character toCharacter(Object d) {
      return (Character) d;
    }

    public Short toShort(Object d) {
      return (Short) d;
    }

    public Integer toInteger(Object d) {
      return (Integer) d;
    }

    public Long toLong(Object d) {
      return (Long) d;
    }

    public Float toFloat(Object d) {
      return (Float) d;
    }

    public Double toDouble(Object d) {
      return (Double) d;
    }

    public String toString(Object d) {
      return (String) d;
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      return (Byte[]) d;
    }

    public Character[] toCharArray(Object d) {
      return (Character[]) d;
    }

    public Short[] toShortArray(Object d) {
      return (Short[]) d;
    }

    public Integer[] toIntegerArray(Object d) {
      return (Integer[]) d;
    }

    public Long[] toLongArray(Object d) {
      return (Long[]) d;
    }

    public Float[] toFloatArray(Object d) {
      return (Float[]) d;
    }

    public Double[] toDoubleArray(Object d) {
      return (Double[]) d;
    }

    public String[] toStringArray(Object d) {
      return (String[]) d;
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
      return (Byte) d;
    }

    public Character toCharacter(Object d) {
      return (Character) d;
    }

    public Short toShort(Object d) {
      return (Short) d;
    }

    public Integer toInteger(Object d) {
      return (Integer) d;
    }

    public Long toLong(Object d) {
      return (Long) d;
    }

    public Float toFloat(Object d) {
      return (Float) d;
    }

    public Double toDouble(Object d) {
      return (Double) d;
    }

    public String toString(Object d) {
      return (String) d;
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      return (Byte[]) d;
    }

    public Character[] toCharArray(Object d) {
      return (Character[]) d;
    }

    public Short[] toShortArray(Object d) {
      return (Short[]) d;
    }

    public Integer[] toIntegerArray(Object d) {
      return (Integer[]) d;
    }

    public Long[] toLongArray(Object d) {
      return (Long[]) d;
    }

    public Float[] toFloatArray(Object d) {
      return (Float[]) d;
    }

    public Double[] toDoubleArray(Object d) {
      return (Double[]) d;
    }

    public String[] toStringArray(Object d) {
      return (String[]) d;
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
      return (Byte) d;
    }

    public Character toCharacter(Object d) {
      return (Character) d;
    }

    public Short toShort(Object d) {
      return (Short) d;
    }

    public Integer toInteger(Object d) {
      return (Integer) d;
    }

    public Long toLong(Object d) {
      return (Long) d;
    }

    public Float toFloat(Object d) {
      return (Float) d;
    }

    public Double toDouble(Object d) {
      return (Double) d;
    }

    public String toString(Object d) {
      return (String) d;
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      return (Byte[]) d;
    }

    public Character[] toCharArray(Object d) {
      return (Character[]) d;
    }

    public Short[] toShortArray(Object d) {
      return (Short[]) d;
    }

    public Integer[] toIntegerArray(Object d) {
      return (Integer[]) d;
    }

    public Long[] toLongArray(Object d) {
      return (Long[]) d;
    }

    public Float[] toFloatArray(Object d) {
      return (Float[]) d;
    }

    public Double[] toDoubleArray(Object d) {
      return (Double[]) d;
    }

    public String[] toStringArray(Object d) {
      return (String[]) d;
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
      return (Byte) d;
    }

    public Character toCharacter(Object d) {
      return (Character) d;
    }

    public Short toShort(Object d) {
      return (Short) d;
    }

    public Integer toInteger(Object d) {
      return (Integer) d;
    }

    public Long toLong(Object d) {
      return (Long) d;
    }

    public Float toFloat(Object d) {
      return (Float) d;
    }

    public Double toDouble(Object d) {
      return (Double) d;
    }

    public String toString(Object d) {
      return (String) d;
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      return (Byte[]) d;
    }

    public Character[] toCharArray(Object d) {
      return (Character[]) d;
    }

    public Short[] toShortArray(Object d) {
      return (Short[]) d;
    }

    public Integer[] toIntegerArray(Object d) {
      return (Integer[]) d;
    }

    public Long[] toLongArray(Object d) {
      return (Long[]) d;
    }

    public Float[] toFloatArray(Object d) {
      return (Float[]) d;
    }

    public Double[] toDoubleArray(Object d) {
      return (Double[]) d;
    }

    public String[] toStringArray(Object d) {
      return (String[]) d;
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
      return (Byte) d;
    }

    public Character toCharacter(Object d) {
      return (Character) d;
    }

    public Short toShort(Object d) {
      return (Short) d;
    }

    public Integer toInteger(Object d) {
      return (Integer) d;
    }

    public Long toLong(Object d) {
      return (Long) d;
    }

    public Float toFloat(Object d) {
      return (Float) d;
    }

    public Double toDouble(Object d) {
      return (Double) d;
    }

    public String toString(Object d) {
      return (String) d;
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      return (Byte[]) d;
    }

    public Character[] toCharArray(Object d) {
      return (Character[]) d;
    }

    public Short[] toShortArray(Object d) {
      return (Short[]) d;
    }

    public Integer[] toIntegerArray(Object d) {
      return (Integer[]) d;
    }

    public Long[] toLongArray(Object d) {
      return (Long[]) d;
    }

    public Float[] toFloatArray(Object d) {
      return (Float[]) d;
    }

    public Double[] toDoubleArray(Object d) {
      return (Double[]) d;
    }

    public String[] toStringArray(Object d) {
      return (String[]) d;
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
      return (Byte) d;
    }

    public Character toCharacter(Object d) {
      return (Character) d;
    }

    public Short toShort(Object d) {
      return (Short) d;
    }

    public Integer toInteger(Object d) {
      return (Integer) d;
    }

    public Long toLong(Object d) {
      return (Long) d;
    }

    public Float toFloat(Object d) {
      return (Float) d;
    }

    public Double toDouble(Object d) {
      return (Double) d;
    }

    public String toString(Object d) {
      return (String) d;
    }

    public Object toObject(Object d) {
      return d;
    }

    public Byte[] toByteArray(Object d) {
      return (Byte[]) d;
    }

    public Character[] toCharArray(Object d) {
      return (Character[]) d;
    }

    public Short[] toShortArray(Object d) {
      return (Short[]) d;
    }

    public Integer[] toIntegerArray(Object d) {
      return (Integer[]) d;
    }

    public Long[] toLongArray(Object d) {
      return (Long[]) d;
    }

    public Float[] toFloatArray(Object d) {
      return (Float[]) d;
    }

    public Double[] toDoubleArray(Object d) {
      return (Double[]) d;
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
    if (fieldSpec.dataType == null) {
      // Only happens in ChaosMonkey test
      return retval;
    }
    switch (fieldSpec.dataType) {
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
