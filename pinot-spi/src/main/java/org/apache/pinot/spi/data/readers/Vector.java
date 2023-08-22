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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import java.util.Arrays;

public class Vector implements Comparable<Vector> {

  public enum VectorType {
    FLOAT(0), INT(1), BYTE(2);

    VectorType(int id) {
      _id = id;
    }

    public int getId() {
      return _id;
    }

    private int _id;

    public static VectorType fromId(int id) {
      switch (id) {
        case 0:
          return FLOAT;
        case 1:
          return INT;
        case 2:
          return BYTE;
        default:
          throw new IllegalArgumentException("Invalid vector type id: " + id);
      }
    }
  }

  private int _dimension;
  private float[] _floatValues;
  private int[] _intValues;
  private byte[] _byteValues;
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

  public Vector(int dimension, byte[] values) {
    _dimension = dimension;
    _byteValues = values;
    _type = VectorType.BYTE;
  }

  public int getDimension() {
    return _dimension;
  }

  public void setDimension(int dimension) {
    _dimension = dimension;
  }

  public Object[] getValues() {
    switch (_type) {
      case FLOAT:
        return new Object[]{_floatValues};
      case INT:
        return new Object[]{_intValues};
      case BYTE:
        return new Object[]{_byteValues};
      default:
        throw new IllegalStateException("Invalid vector type: " + _type);
    }
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

  @JsonIgnore
  public byte[] getByteValues() {
    if (_type != VectorType.BYTE) {
      throw new IllegalStateException("Vector type is not BYTE");
    }
    return _byteValues;
  }

  public void setValues(float[] values) {
    _floatValues = values;
    _type = VectorType.FLOAT;
  }

  public void setValues(int[] values) {
    _intValues = values;
    _type = VectorType.INT;
  }

  public void setValues(byte[] values) {
    _byteValues = values;
    _type = VectorType.BYTE;
  }

  public VectorType getType() {
    return _type;
  }

  public static Vector fromString(String value) {
    String[] tokens = value.split(",");

    //TODO: This is a hack to support null vectors.
    if (Integer.parseInt(tokens[0].toUpperCase()) == -1) {
      return new Vector(0, new float[0]);
    }

    VectorType vectorType = VectorType.fromId(Integer.parseInt(tokens[0].toUpperCase()));
    int dimension = Integer.parseInt(tokens[1]);
    switch (vectorType) {
      case FLOAT:
        float[] floatResult = new float[tokens.length - 2];
        for (int i = 2; i < tokens.length; i++) {
          floatResult[i - 2] = Float.parseFloat(tokens[i]);
        }
        return new Vector(dimension, floatResult);
      case INT:
        int[] intResult = new int[tokens.length - 2];
        for (int i = 2; i < tokens.length; i++) {
          intResult[i - 2] = Integer.parseInt(tokens[i]);
        }
        return new Vector(dimension, intResult);
      case BYTE:
        byte[] byteResult = new byte[tokens.length - 2];
        for (int i = 2; i < tokens.length; i++) {
          byteResult[i - 2] = Byte.parseByte(tokens[i]);
        }
        return new Vector(dimension, byteResult);
      default:
        throw new IllegalArgumentException("Unsupported vector type: " + vectorType);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(_type.getId());
    sb.append(",");
    sb.append(_dimension);
    sb.append(",");
    switch (_type) {
      case FLOAT:
        for (float value : _floatValues) {
          sb.append(value).append(",");
        }
        break;
      case INT:
        for (int value : _intValues) {
          sb.append(value).append(",");
        }
        break;
      case BYTE:
        for (byte value : _byteValues) {
          sb.append(value).append(",");
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported vector type: " + _type);
    }
    return sb.substring(0, sb.length() - 1); // To remove the last comma
  }

  public byte[] toBytes() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      dos.writeInt(_type.getId());
      dos.writeInt(_dimension);
      switch (_type) {
        case FLOAT:
          for (float value : _floatValues) {
            dos.writeFloat(value);
          }
          break;
        case INT:
          for (int value : _intValues) {
            dos.writeInt(value);
          }
          break;
        case BYTE:
          for (byte value : _byteValues) {
            dos.writeByte(value);
          }
          break;
        default:
          throw new IllegalArgumentException("Unsupported vector type: " + _type);
      }
    } catch (IOException e) {
      // This is unlikely since we're writing to a byte array stream,
      // but you may want to handle this differently.
      throw new RuntimeException(e);
    }
    return baos.toByteArray();
  }

  public static Vector fromBytes(byte[] data) {
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    DataInputStream dis = new DataInputStream(bais);
    try {
      int typeId = dis.readInt();
      VectorType type = VectorType.fromId(typeId);
      int dimension = dis.readInt();
      switch (type) {
        case FLOAT:
          float[] floatValues = new float[dimension];
          for (int i = 0; i < dimension; i++) {
            floatValues[i] = dis.readFloat();
          }
          return new Vector(dimension, floatValues);
        case INT:
          int[] intValues = new int[dimension];
          for (int i = 0; i < dimension; i++) {
            intValues[i] = dis.readInt();
          }
          return new Vector(dimension, intValues);
        case BYTE:
          byte[] byteValues = new byte[dimension];
          for (int i = 0; i < dimension; i++) {
            byteValues[i] = dis.readByte();
          }
          return new Vector(dimension, byteValues);
        default:
          throw new IllegalArgumentException("Unsupported serialized vector type: " + type);
      }
    } catch (IOException e) {
      // This is unlikely since we're reading from a byte array stream,
      // but you may want to handle this differently.
      throw new RuntimeException(e);
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
    } else if (_type == VectorType.INT) {
      for (int i = 0; i < _intValues.length; i++) {
        if (_intValues[i] != other._intValues[i]) {
          return Integer.compare(_intValues[i], other._intValues[i]);
        }
      }
    } else if (_type == VectorType.BYTE) {
      for (int i = 0; i < _byteValues.length; i++) {
        if (_byteValues[i] != other._byteValues[i]) {
          return Byte.compare(_byteValues[i], other._byteValues[i]);
        }
      }
    }
    return 0;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    Vector vector = (Vector) other;
    if (_dimension != vector._dimension || _type != vector._type) return false;
    if (_type == VectorType.FLOAT) return Arrays.equals(_floatValues, vector._floatValues);
    if (_type == VectorType.INT) return Arrays.equals(_intValues, vector._intValues);
    return Arrays.equals(_byteValues, vector._byteValues);
  }

  @Override
  public int hashCode() {
    int result = _type.hashCode();
    result = 31 * result + _dimension;
    if (_type == VectorType.FLOAT) {
      result = 31 * result + Arrays.hashCode(_floatValues);
    } else if (_type == VectorType.INT) {
      result = 31 * result + Arrays.hashCode(_intValues);
    } else if (_type == VectorType.BYTE) {
      result = 31 * result + Arrays.hashCode(_byteValues);
    }
    return result;
  }
}
