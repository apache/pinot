package org.apache.pinot.spi.utils;

public class VectorUtils {
  private VectorUtils() {
  }

  public static String toString(float[] value) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < value.length; i++) {
      sb.append(value[i]);
      if (i < value.length - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  public static float[] fromString(String value) {
    String[] tokens = value.substring(1, value.length() - 1).split(",");
    float[] result = new float[tokens.length];
    for (int i = 0; i < tokens.length; i++) {
      result[i] = Float.parseFloat(tokens[i]);
    }
    return result;
  }
}
