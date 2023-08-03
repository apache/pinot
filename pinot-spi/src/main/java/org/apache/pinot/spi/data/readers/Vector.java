package org.apache.pinot.spi.data.readers;

import java.util.Arrays;


public class Vector implements Comparable<Vector> {
  public enum VectorType {
    FLOAT, INT
  }

  private int dimension;
  private float[] floatValues;
  private int[] intValues;
  private VectorType type;

  public Vector(int dimension, float[] values) {
    this.dimension = dimension;
    this.floatValues = values;
    this.type = VectorType.FLOAT;
  }

  public Vector(int dimension, int[] values) {
    this.dimension = dimension;
    this.intValues = values;
    this.type = VectorType.INT;
  }

  public int getDimension() {
    return dimension;
  }

  public void setDimension(int dimension) {
    this.dimension = dimension;
  }

  public float[] getFloatValues() {
    if (type != VectorType.FLOAT) {
      throw new IllegalStateException("Vector type is not FLOAT");
    }
    return floatValues;
  }

  public int[] getIntValues() {
    if (type != VectorType.INT) {
      throw new IllegalStateException("Vector type is not INT");
    }
    return intValues;
  }

  public void setValues(float[] values) {
    this.floatValues = values;
    this.type = VectorType.FLOAT;
  }

  public void setValues(int[] values) {
    this.intValues = values;
    this.type = VectorType.INT;
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

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(dimension);
    sb.append(",");
    switch (type) {
      case FLOAT:
        for (int i = 0; i < floatValues.length; i++) {
          sb.append(floatValues[i]);
          if (i < floatValues.length - 1) {
            sb.append(",");
          }
        }
        break;
      case INT:
        for (int i = 0; i < intValues.length; i++) {
          sb.append(intValues[i]);
          if (i < intValues.length - 1) {
            sb.append(",");
          }
        }
        break;
    }
    return sb.toString();
  }

  public byte[] toBytes() {
    int size = type == VectorType.FLOAT ? floatValues.length : intValues.length;
    byte[] result = new byte[5 + 4 * size]; // 1 byte for type, 4 for dimension
    int offset = 0;
    result[offset++] = (byte) (type == VectorType.FLOAT ? 0 : 1);
    int intBits = dimension;
    result[offset++] = (byte) (intBits >> 24);
    result[offset++] = (byte) (intBits >> 16);
    result[offset++] = (byte) (intBits >> 8);
    result[offset++] = (byte) (intBits);
    switch (type) {
      case FLOAT:
        for (int i = 0; i < floatValues.length; i++) {
          intBits = Float.floatToIntBits(floatValues[i]);
          result[offset++] = (byte) (intBits >> 24);
          result[offset++] = (byte) (intBits >> 16);
          result[offset++] = (byte) (intBits >> 8);
          result[offset++] = (byte) (intBits);
        }
        break;
      case INT:
        for (int i = 0; i < intValues.length; i++) {
          intBits = intValues[i];
          result[offset++] = (byte) (intBits >> 24);
          result[offset++] = (byte) (intBits >> 16);
          result[offset++] = (byte) (intBits >> 8);
          result[offset++] = (byte) (intBits);
        }
        break;
    }
    return result;
  }

  public static Vector fromBytes(byte[] bytes) {
    int offset = 0;
    byte typeByte = bytes[offset++];
    VectorType type = typeByte == 0 ? VectorType.FLOAT : VectorType.INT;
    int intBits = (bytes[offset++] & 0xFF) << 24;
    intBits |= (bytes[offset++] & 0xFF) << 16;
    intBits |= (bytes[offset++] & 0xFF) << 8;
    intBits |= (bytes[offset++] & 0xFF);
    int dimension = intBits;

    int size = (bytes.length - 5) / 4;
    float[] floatResult = null;
    int[] intResult = null;

    switch (type) {
      case FLOAT:
        floatResult = new float[size];
        for (int i = 0; i < floatResult.length; i++) {
          intBits = (bytes[offset++] & 0xFF) << 24;
          intBits |= (bytes[offset++] & 0xFF) << 16;
          intBits |= (bytes[offset++] & 0xFF) << 8;
          intBits |= (bytes[offset++] & 0xFF);
          floatResult[i] = Float.intBitsToFloat(intBits);
        }
        return new Vector(dimension, floatResult);
      case INT:
        intResult = new int[size];
        for (int i = 0; i < intResult.length; i++) {
          intBits = (bytes[offset++] & 0xFF) << 24;
          intBits |= (bytes[offset++] & 0xFF) << 16;
          intBits |= (bytes[offset++] & 0xFF) << 8;
          intBits |= (bytes[offset++] & 0xFF);
          intResult[i] = intBits;
        }
        return new Vector(dimension, intResult);
    }
    return null; // Should never reach here
  }

  @Override
  public int compareTo(Vector other) {
    if (this.dimension != other.dimension) {
      return this.dimension - other.dimension;
    }
    if (this.type != other.type) {
      throw new IllegalArgumentException("Cannot compare vectors of different types");
    }
    if (this.type == VectorType.FLOAT) {
      for (int i = 0; i < this.floatValues.length; i++) {
        if (this.floatValues[i] != other.floatValues[i]) {
          return Float.compare(this.floatValues[i], other.floatValues[i]);
        }
      }
    } else {
      for (int i = 0; i < this.intValues.length; i++) {
        if (this.intValues[i] != other.intValues[i]) {
          return Integer.compare(this.intValues[i], other.intValues[i]);
        }
      }
    }
    return 0;
  }

  public boolean equals(Vector other) {
    if (this.dimension != other.dimension || this.type != other.type) {
      return false;
    }
    if (this.type == VectorType.FLOAT) {
      return Arrays.equals(this.floatValues, other.floatValues);
    } else {
      return Arrays.equals(this.intValues, other.intValues);
    }
  }
}
