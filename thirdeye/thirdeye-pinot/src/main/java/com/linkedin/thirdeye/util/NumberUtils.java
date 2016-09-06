package com.linkedin.thirdeye.util;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.linkedin.thirdeye.api.MetricType;

/**
 * Helper class to convert to and from Number data type. - supports arithmetic
 * on two number data types
 * @author kgopalak
 */
public class NumberUtils {

  public static Number sum(Number a, Number b, MetricType type) {
    switch (type) {
    case SHORT:
      return a.shortValue() + b.shortValue();
    case INT:
      return a.intValue() + b.intValue();
    case LONG:
      return a.longValue() + b.longValue();
    case FLOAT:
      return a.floatValue() + b.floatValue();
    case DOUBLE:
      return a.doubleValue() + b.doubleValue();
    default:
      return null;
    }
  }

  public static Number difference(Number a, Number b, MetricType type) {
    switch (type) {
    case SHORT:
      return a.shortValue() - b.shortValue();
    case INT:
      return a.intValue() - b.intValue();
    case LONG:
      return a.longValue() - b.longValue();
    case FLOAT:
      return a.floatValue() - b.floatValue();
    case DOUBLE:
      return a.doubleValue() - b.doubleValue();
    default:
      return null;
    }
  }

  public static void addToBuffer(ByteBuffer buffer, Number value, MetricType type) {
    switch (type) {
    case SHORT:
      buffer.putShort(value.shortValue());
      break;
    case INT:
      buffer.putInt(value.intValue());
      break;
    case LONG:
      buffer.putLong(value.longValue());
      break;
    case FLOAT:
      buffer.putFloat(value.floatValue());
      break;
    case DOUBLE:
      buffer.putDouble(value.doubleValue());
      break;
    }
  }

  public static Number readFromBuffer(ByteBuffer buffer, MetricType type) {
    switch (type) {
    case SHORT:
      return buffer.getShort();
    case INT:
      return buffer.getInt();
    case LONG:
      return buffer.getLong();
    case FLOAT:
      return buffer.getFloat();
    case DOUBLE:
      return buffer.getDouble();
    }
    return null;
  }

  public static void addToDataOutputStream(DataOutputStream dataOutputStream, Number value,
      MetricType type) throws IOException {
    switch (type) {
    case SHORT:
      dataOutputStream.writeShort(value.shortValue());
      break;
    case INT:
      dataOutputStream.writeInt(value.intValue());
      break;
    case LONG:
      dataOutputStream.writeLong(value.longValue());
      break;
    case FLOAT:
      dataOutputStream.writeFloat(value.floatValue());
      break;
    case DOUBLE:
      dataOutputStream.writeDouble(value.doubleValue());
      break;
    }
  }

  // TODO: Remove
  public static int byteSize(MetricType type) {
    return type.byteSize();
  }

  public static Number divide(Number numerator, Number denominator, MetricType type) {
    switch (type) {
    case SHORT:
      return numerator.shortValue() / denominator.shortValue();
    case INT:
      return numerator.intValue() / denominator.intValue();
    case LONG:
      return numerator.longValue() / denominator.longValue();
    case FLOAT:
      return numerator.floatValue() / denominator.floatValue();
    case DOUBLE:
      return numerator.doubleValue() / denominator.doubleValue();
    }
    return -1;
  }

  public static boolean isZero(Number value, MetricType type) {
    switch (type) {
    case SHORT:
      return value.shortValue() == 0;
    case INT:
      return value.intValue() == 0;
    case LONG:
      return value.longValue() == 0L;
    case FLOAT:
      return value.floatValue() == 0.0;
    case DOUBLE:
      return value.doubleValue() == 0.0;
    }
    throw new IllegalArgumentException("Invalid type " + type);
  }

  public static Number valueOf(String value, MetricType type) {
    switch (type) {
    case SHORT:
      return Short.valueOf(value);
    case INT:
      return Integer.valueOf(value);
    case LONG:
      return Long.valueOf(value);
    case FLOAT:
      return Float.valueOf(value);
    case DOUBLE:
      return Double.valueOf(value);
    }
    throw new IllegalArgumentException("Invalid type " + type);
  }
}
