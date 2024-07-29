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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * The primary key of a record. Note that the value used in the primary key must be single-value.
 */
public final class PrimaryKey {
  private final Object[] _values;

  public PrimaryKey(Object[] values) {
    _values = values;
  }

  public Object[] getValues() {
    return _values;
  }

  public byte[] asBytes() {
    if (_values.length == 1) {
      return asBytesSingleVal(_values[0]);
    }

    int sizeInBytes = 0;
    byte[][] cache = new byte[_values.length][];
    for (int i = 0; i < _values.length; i++) {
      Object value = _values[i];

      if (value instanceof Integer) {
        sizeInBytes += Integer.BYTES;
      } else if (value instanceof Long) {
        sizeInBytes += Long.BYTES;
      } else if (value instanceof String) {
        cache[i] = ((String) value).getBytes(StandardCharsets.UTF_8);
        sizeInBytes += cache[i].length + Integer.BYTES;
      } else if (value instanceof ByteArray) {
        cache[i] = ((ByteArray) value).getBytes();
        sizeInBytes += cache[i].length + Integer.BYTES;
      } else if (value instanceof Float) {
        sizeInBytes += Float.BYTES;
      } else if (value instanceof Double) {
        sizeInBytes += Double.BYTES;
      } else if (value instanceof BigDecimal) {
        cache[i] = BigDecimalUtils.serialize((BigDecimal) value);
        sizeInBytes += cache[i].length + Integer.BYTES;
      } else {
        throw new IllegalStateException(
            String.format("Unsupported value: %s of type: %s", value, value != null ? value.getClass() : null));
      }
    }

    ByteBuffer byteBuffer = ByteBuffer.allocate(sizeInBytes);
    for (int i = 0; i < _values.length; i++) {
      Object value = _values[i];

      if (value instanceof Integer) {
        byteBuffer.putInt((Integer) value);
      } else if (value instanceof Long) {
        byteBuffer.putLong((Long) value);
      } else if (value instanceof Float) {
        byteBuffer.putFloat((Float) value);
      } else if (value instanceof Double) {
        byteBuffer.putDouble((Double) value);
      } else {
        byteBuffer.putInt(cache[i].length);
        byteBuffer.put(cache[i]);
      }
    }
    return byteBuffer.array();
  }

  private byte[] asBytesSingleVal(Object value) {
    if (value instanceof Integer) {
      ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
      byteBuffer.putInt((Integer) value);
      return byteBuffer.array();
    } else if (value instanceof Long) {
      ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);
      byteBuffer.putLong((Long) value);
      return byteBuffer.array();
    } else if (value instanceof String) {
      return ((String) value).getBytes(StandardCharsets.UTF_8);
    } else if (value instanceof ByteArray) {
      return ((ByteArray) value).getBytes();
    } else if (value instanceof Float) {
      ByteBuffer byteBuffer = ByteBuffer.allocate(Float.BYTES);
      byteBuffer.putFloat((Float) value);
      return byteBuffer.array();
    } else if (value instanceof Double) {
      ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES);
      byteBuffer.putDouble((Double) value);
      return byteBuffer.array();
    } else if (value instanceof BigDecimal) {
      return BigDecimalUtils.serialize((BigDecimal) value);
    } else {
      throw new IllegalStateException(
          String.format("Unsupported value: %s of type: %s", value, value != null ? value.getClass() : null));
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof PrimaryKey) {
      PrimaryKey that = (PrimaryKey) obj;
      return Arrays.equals(_values, that._values);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(_values);
  }

  @Override
  public String toString() {
    return Arrays.toString(_values);
  }
}
