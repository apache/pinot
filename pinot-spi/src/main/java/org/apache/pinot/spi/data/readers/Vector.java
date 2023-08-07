/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.data.readers;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Arrays;


public class Vector implements Comparable<Vector> {
  public enum VectorType {
    FLOAT, INT
  }

  private int _dimension;
  private float[] _floatValues;
  private int[] _intValues;
  private VectorType _type;

  public Vector(int dimension, float[] values) {
    _dimension = dimension;
    _floatValues = values;
    _type = VectorType.FLOAT;
  }

  public Vector(int dimension, int[] values) {
    _dimension = dimension;
    _intValues = values;
    _type = VectorType.INT;
  }

  public int getDimension() {
    return _dimension;
  }

  public void setDimension(int dimension) {
    _dimension = dimension;
  }

  @JsonIgnore
  public float[] getFloatValues() {
    if (_type != VectorType.FLOAT) {
      throw new IllegalStateException("Vector type is not FLOAT");
    }
    return _floatValues;
  }

  @JsonIgnore
  public int[] getIntValues() {
    if (_type != VectorType.INT) {
      throw new IllegalStateException("Vector type is not INT");
    }
    return _intValues;
  }

  public void setValues(float[] values) {
    _floatValues = values;
    _type = VectorType.FLOAT;
  }

  public void setValues(int[] values) {
    _intValues = values;
    _type = VectorType.INT;
  }

  public VectorType getType() {
    return _type;
  }

  public static Vector fromString(String value) {
    String[] tokens = value.split(",");
    VectorType vectorType = VectorType.valueOf(tokens[0].toUpperCase());
    int dimension = Integer.parseInt(tokens[1]);
    switch (vectorType) {
      case FLOAT: {
        float[] result = new float[tokens.length - 2];
        for (int i = 2; i < tokens.length; i++) {
          result[i - 2] = Float.parseFloat(tokens[i]);
        }
        return new Vector(dimension, result);
      }
      case INT: {
        int[] result = new int[tokens.length - 2];
        for (int i = 2; i < tokens.length; i++) {
          result[i - 2] = Integer.parseInt(tokens[i]);
        }
        return new Vector(dimension, result);
      }
      default:
        throw new IllegalArgumentException("Unsupported vector type: " + vectorType);
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(_type);
    sb.append(",");
    sb.append(_dimension);
    sb.append(",");
    switch (_type) {
      case FLOAT:
        for (int i = 0; i < _floatValues.length; i++) {
          sb.append(_floatValues[i]);
          if (i < _floatValues.length - 1) {
            sb.append(",");
          }
        }
        break;
      case INT:
        for (int i = 0; i < _intValues.length; i++) {
          sb.append(_intValues[i]);
          if (i < _intValues.length - 1) {
            sb.append(",");
          }
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported vector type: " + _type);
    }
    return sb.toString();
  }

  public byte[] toBytes() {
    int size = _type == VectorType.FLOAT ? _floatValues.length : _intValues.length;
    byte[] result = new byte[5 + 4 * size]; // 1 byte for type, 4 for dimension
    int offset = 0;
    result[offset++] = (byte) (_type == VectorType.FLOAT ? 0 : 1);
    int intBits = _dimension;
    result[offset++] = (byte) (intBits >> 24);
    result[offset++] = (byte) (intBits >> 16);
    result[offset++] = (byte) (intBits >> 8);
    result[offset++] = (byte) (intBits);
    switch (_type) {
      case FLOAT:
        for (int i = 0; i < _floatValues.length; i++) {
          intBits = Float.floatToIntBits(_floatValues[i]);
          result[offset++] = (byte) (intBits >> 24);
          result[offset++] = (byte) (intBits >> 16);
          result[offset++] = (byte) (intBits >> 8);
          result[offset++] = (byte) (intBits);
        }
        break;
      case INT:
        for (int i = 0; i < _intValues.length; i++) {
          intBits = _intValues[i];
          result[offset++] = (byte) (intBits >> 24);
          result[offset++] = (byte) (intBits >> 16);
          result[offset++] = (byte) (intBits >> 8);
          result[offset++] = (byte) (intBits);
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported vector type: " + _type);
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
      default:
        throw new IllegalArgumentException("Unsupported vector type: " + type);
    }
  }

  @Override
  public int compareTo(Vector other) {
    if (_dimension != other._dimension) {
      return _dimension - other._dimension;
    }
    if (_type != other._type) {
      throw new IllegalArgumentException("Cannot compare vectors of different types");
    }
    if (_type == VectorType.FLOAT) {
      for (int i = 0; i < _floatValues.length; i++) {
        if (_floatValues[i] != other._floatValues[i]) {
          return Float.compare(_floatValues[i], other._floatValues[i]);
        }
      }
    } else {
      for (int i = 0; i < _intValues.length; i++) {
        if (_intValues[i] != other._intValues[i]) {
          return Integer.compare(_intValues[i], other._intValues[i]);
        }
      }
    }
    return 0;
  }

  @Override
  public boolean equals(Object other) {
    Vector otherVector = (Vector) other;
    if (_dimension != otherVector._dimension || _type != otherVector._type) {
      return false;
    }
    if (_type == VectorType.FLOAT) {
      return Arrays.equals(_floatValues, otherVector._floatValues);
    } else {
      return Arrays.equals(_intValues, otherVector._intValues);
    }
  }

  @Override
  public int hashCode() {
    int result = _type.hashCode();
    result = 31 * result + _dimension;
    if (_type == VectorType.FLOAT) {
      result = 31 * result + Arrays.hashCode(_floatValues);
    } else {
      result = 31 * result + Arrays.hashCode(_intValues);
    }
    return result;
  }
}
