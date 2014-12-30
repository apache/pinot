package com.linkedin.thirdeye.impl;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Helper class to convert to and from Number data type. - supports arithmetic
 * on two number data types
 * 
 * @author kgopalak
 * 
 */
public class NumberUtils {

  public static Number sum(Number a, Number b, String type) {
    if ("SHORT".equals(type)) {
      return a.shortValue() + b.shortValue();
    }
    if ("INT".equals(type)) {
      return a.intValue() + b.intValue();
    }
    if ("LONG".equals(type)) {
      return a.longValue() + b.longValue();
    }
    if ("FLOAT".equals(type)) {
      return a.floatValue() + b.floatValue();
    }
    if ("DOUBLE".equals(type)) {
      return a.doubleValue() + b.doubleValue();
    }
    return null;
  }

  public static void addToBuffer(ByteBuffer buffer, Number value, String type) {
    if ("SHORT".equals(type)) {
      buffer.putShort(value.shortValue());
    }
    if ("INT".equals(type)) {
      buffer.putInt(value.intValue());
    }
    if ("LONG".equals(type)) {
      buffer.putLong(value.longValue());
    }
    if ("FLOAT".equals(type)) {
      buffer.putFloat(value.floatValue());
    }
    if ("DOUBLE".equals(type)) {
      buffer.putDouble(value.doubleValue());
    }
  }

  public static Number readFromBuffer(ByteBuffer buffer, String type) {
    if ("SHORT".equals(type)) {
      return buffer.getShort();
    }
    if ("INT".equals(type)) {
      return buffer.getInt();
    }
    if ("LONG".equals(type)) {
      return buffer.getLong();
    }
    if ("FLOAT".equals(type)) {
      return buffer.getFloat();
    }
    if ("DOUBLE".equals(type)) {
      return buffer.getDouble();
    }
    return null;
  }

  public static void addToDataOutputStream(DataOutputStream dataOutputStream,
      Number value, String type) throws IOException {
    if ("SHORT".equals(type)) {
      dataOutputStream.writeShort(value.shortValue());
    }
    if ("INT".equals(type)) {
      dataOutputStream.writeInt(value.intValue());
    }
    if ("LONG".equals(type)) {
      dataOutputStream.writeLong(value.longValue());
    }
    if ("FLOAT".equals(type)) {
      dataOutputStream.writeFloat(value.floatValue());
    }
    if ("DOUBLE".equals(type)) {
      dataOutputStream.writeDouble(value.doubleValue());
    }

  }

  public static int byteSize(String type) {
    if ("SHORT".equals(type)) {
      return 2;
    } else if ("INT".equals(type)) {
      return 4;
    } else if ("LONG".equals(type)) {
      return 8;
    } else if ("FLOAT".equals(type)) {
      return 4;
    } else if ("DOUBLE".equals(type)) {
      return 8;
    }
    return 0;
  }

}
