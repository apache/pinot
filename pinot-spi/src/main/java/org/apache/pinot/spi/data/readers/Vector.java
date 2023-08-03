package org.apache.pinot.spi.data.readers;

public class Vector implements Comparable {

  private int dimension;
  private float[] values;

  public Vector(int dimension, float[] values) {
    this.dimension = dimension;
    this.values = values;
  }

  public int getDimension() {
    return dimension;
  }

  public void setDimension(int dimension) {
    this.dimension = dimension;
  }

  public float[] getValues() {
    return values;
  }

  public void setValues(float[] values) {
    this.values = values;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(dimension);
    sb.append(",");
    for (int i = 0; i < values.length; i++) {
      sb.append(values[i]);
      if (i < values.length - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }

  public static Vector fromString(String value) {
    String[] tokens = value.split(",");
    int dimension = Integer.parseInt(tokens[0]);
    float[] result = new float[tokens.length - 1];
    for (int i = 1; i < tokens.length; i++) {
      result[i - 1] = Float.parseFloat(tokens[i]);
    }
    return new Vector(dimension, result);
  }

  public byte[] toBytes() {
    byte[] result = new byte[4 * (values.length + 1)];
    int offset = 0;
    for (int i = 0; i < values.length; i++) {
      int intBits = Float.floatToIntBits(values[i]);
      result[offset++] = (byte) (intBits >> 24);
      result[offset++] = (byte) (intBits >> 16);
      result[offset++] = (byte) (intBits >> 8);
      result[offset++] = (byte) (intBits);
    }
    int intBits = dimension;
    result[offset++] = (byte) (intBits >> 24);
    result[offset++] = (byte) (intBits >> 16);
    result[offset++] = (byte) (intBits >> 8);
    result[offset++] = (byte) (intBits);
    return result;
  }

  public static Vector fromBytes(byte[] bytes) {
    float[] result = new float[(bytes.length - 4) / 4];
    int offset = 0;
    for (int i = 0; i < result.length; i++) {
      int intBits = (bytes[offset++] & 0xFF) << 24;
      intBits |= (bytes[offset++] & 0xFF) << 16;
      intBits |= (bytes[offset++] & 0xFF) << 8;
      intBits |= (bytes[offset++] & 0xFF);
      result[i] = Float.intBitsToFloat(intBits);
    }
    int intBits = (bytes[offset++] & 0xFF) << 24;
    intBits |= (bytes[offset++] & 0xFF) << 16;
    intBits |= (bytes[offset++] & 0xFF) << 8;
    intBits |= (bytes[offset++] & 0xFF);
    int dimension = intBits;
    return new Vector(dimension, result);
  }

  @Override
  public int compareTo(Object o) {
    Vector other = (Vector) o;
    if (this.dimension != other.dimension) {
      return this.dimension - other.dimension;
    }
    for (int i = 0; i < this.values.length; i++) {
      if (this.values[i] != other.values[i]) {
        return (int) (this.values[i] - other.values[i]);
      }
    }
    return 0;
  }

  public boolean equals(Vector other) {
    if (this.dimension != other.dimension) {
      return false;
    }
    for (int i = 0; i < this.values.length; i++) {
      if (this.values[i] != other.values[i]) {
        return false;
      }
    }
    return true;
  }
}
