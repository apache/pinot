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
package com.linkedin.thirdeye.hadoop.config;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Represents the various data types supported for a dimension<br/>
 * Currently we support INT, SHORT, LONG, FLOAT, DOUBLE, STRING, BOOLEAN
 */
public enum DimensionType {
  INT {
    @Override
    public Object getValueFromString(String strVal) {
      return Integer.valueOf(strVal);
    }

    @Override
    public Object getDefaultNullvalue() {
      return ThirdEyeConstants.EMPTY_INT;
    }

    @Override
    public Object getDefaultOtherValue() {
      return ThirdEyeConstants.EMPTY_INT;
    }
  },
  SHORT {
    @Override
    public Object getValueFromString(String strVal) {
      return Short.valueOf(strVal);
    }

    @Override
    public Object getDefaultNullvalue() {
      return ThirdEyeConstants.EMPTY_SHORT;
    }

    @Override
    public Object getDefaultOtherValue() {
      return ThirdEyeConstants.EMPTY_SHORT;
    }
  },
  LONG {
    @Override
    public Object getValueFromString(String strVal) {
      return Long.valueOf(strVal);
    }

    @Override
    public Object getDefaultNullvalue() {
      return ThirdEyeConstants.EMPTY_LONG;
    }

    @Override
    public Object getDefaultOtherValue() {
      return ThirdEyeConstants.EMPTY_LONG;
    }
  },
  FLOAT {
    @Override
    public Object getValueFromString(String strVal) {
      return Float.valueOf(strVal);
    }

    @Override
    public Object getDefaultNullvalue() {
      return ThirdEyeConstants.EMPTY_FLOAT;
    }

    @Override
    public Object getDefaultOtherValue() {
      return ThirdEyeConstants.EMPTY_FLOAT;
    }
  },
  DOUBLE {
    @Override
    public Object getValueFromString(String strVal) {
      return Double.valueOf(strVal);
    }

    @Override
    public Object getDefaultNullvalue() {
      return ThirdEyeConstants.EMPTY_DOUBLE;
    }

    @Override
    public Object getDefaultOtherValue() {
      return ThirdEyeConstants.EMPTY_DOUBLE;
    }
  },
  STRING {
    @Override
    public Object getValueFromString(String strVal) {
      return strVal;
    }

    @Override
    public Object getDefaultNullvalue() {
      return ThirdEyeConstants.EMPTY_STRING;
    }

    @Override
    public Object getDefaultOtherValue() {
      return ThirdEyeConstants.OTHER;
    }
  };


  public abstract Object getValueFromString(String strVal);

  public abstract Object getDefaultNullvalue();

  public abstract Object getDefaultOtherValue();


  /**
   * Writes the dimension value to a data outputstream
   * @param dos DataOutputStream
   * @param dimensionValue
   * @param dimensionType
   * @throws IOException
   */
  public static void writeDimensionValueToOutputStream(DataOutputStream dos, Object dimensionValue,
      DimensionType dimensionType) throws IOException {
    switch (dimensionType) {
    case DOUBLE:
      dos.writeDouble((double) dimensionValue);
      break;
    case FLOAT:
      dos.writeFloat((float) dimensionValue);
      break;
    case INT:
      dos.writeInt((int) dimensionValue);
      break;
    case LONG:
      dos.writeLong((long) dimensionValue);
      break;
    case SHORT:
      dos.writeShort((short) dimensionValue);
      break;
    case STRING:
      String stringVal = (String) dimensionValue;
      byte[] bytes = stringVal.getBytes();
      dos.writeInt(bytes.length);
      dos.write(bytes);
      break;
    default:
      throw new IllegalArgumentException("Unsupported dimensionType " + dimensionType);
    }
  }

  /**
   * Reads the dimension value from a given data input stream
   * @param dis DataInputStream
   * @param dimensionType
   * @return
   * @throws IOException
   */
  public static Object readDimensionValueFromDataInputStream(DataInputStream dis, DimensionType dimensionType) throws IOException {
    Object dimensionValue = null;
    switch (dimensionType) {
    case DOUBLE:
      dimensionValue = dis.readDouble();
      break;
    case FLOAT:
      dimensionValue = dis.readFloat();
      break;
    case INT:
      dimensionValue = dis.readInt();
      break;
    case SHORT:
      dimensionValue = dis.readShort();
      break;
    case LONG:
      dimensionValue = dis.readLong();
      break;
    case STRING:
      int length = dis.readInt();
      byte[] bytes = new byte[length];
      dis.read(bytes);
      dimensionValue = new String(bytes);
      break;
    default:
      throw new IllegalArgumentException("Unsupported dimensionType " + dimensionType);
    }
    return dimensionValue;
  }

}
