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
package org.apache.pinot.segment.spi.partition.pipeline;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;


/**
 * Typed runtime value used within the compiled partition pipeline.
 */
public final class PartitionValue {
  private final PartitionValueType _type;
  private final Object _value;

  private PartitionValue(PartitionValueType type, Object value) {
    _type = type;
    _value = value;
  }

  public static PartitionValue stringValue(String value) {
    Preconditions.checkNotNull(value, "Partition pipeline does not accept null STRING values");
    return new PartitionValue(PartitionValueType.STRING, value);
  }

  public static PartitionValue bytesValue(byte[] value) {
    Preconditions.checkNotNull(value, "Partition pipeline does not accept null BYTES values");
    return new PartitionValue(PartitionValueType.BYTES, value.clone());
  }

  public static PartitionValue intValue(int value) {
    return new PartitionValue(PartitionValueType.INT, value);
  }

  public static PartitionValue longValue(long value) {
    return new PartitionValue(PartitionValueType.LONG, value);
  }

  public static PartitionValue floatValue(float value) {
    return new PartitionValue(PartitionValueType.FLOAT, value);
  }

  public static PartitionValue doubleValue(double value) {
    return new PartitionValue(PartitionValueType.DOUBLE, value);
  }

  public PartitionValueType getType() {
    return _type;
  }

  public String getStringValue() {
    Preconditions.checkState(_type == PartitionValueType.STRING, "Expected STRING value but got: %s", _type);
    return (String) _value;
  }

  public byte[] getBytesValue() {
    Preconditions.checkState(_type == PartitionValueType.BYTES, "Expected BYTES value but got: %s", _type);
    return ((byte[]) _value).clone();
  }

  public int getIntValue() {
    Preconditions.checkState(_type == PartitionValueType.INT, "Expected INT value but got: %s", _type);
    return (Integer) _value;
  }

  public long getLongValue() {
    Preconditions.checkState(_type == PartitionValueType.LONG, "Expected LONG value but got: %s", _type);
    return (Long) _value;
  }

  public float getFloatValue() {
    Preconditions.checkState(_type == PartitionValueType.FLOAT, "Expected FLOAT value but got: %s", _type);
    return (Float) _value;
  }

  public double getDoubleValue() {
    Preconditions.checkState(_type == PartitionValueType.DOUBLE, "Expected DOUBLE value but got: %s", _type);
    return (Double) _value;
  }

  public Object toObject() {
    switch (_type) {
      case STRING:
        return getStringValue();
      case BYTES:
        return getBytesValue();
      case INT:
        return getIntValue();
      case LONG:
        return getLongValue();
      case FLOAT:
        return getFloatValue();
      case DOUBLE:
        return getDoubleValue();
      default:
        throw new IllegalStateException("Unsupported partition pipeline runtime value type: " + _type);
    }
  }

  public static PartitionValue fromObject(Object value) {
    Preconditions.checkNotNull(value, "Partition pipeline function returned null");
    if (value instanceof String) {
      return stringValue((String) value);
    }
    if (value instanceof byte[]) {
      return bytesValue((byte[]) value);
    }
    if (value instanceof Integer) {
      return intValue((Integer) value);
    }
    if (value instanceof Long) {
      return longValue((Long) value);
    }
    if (value instanceof Float) {
      return floatValue((Float) value);
    }
    if (value instanceof Double) {
      return doubleValue((Double) value);
    }
    throw new IllegalArgumentException("Unsupported partition pipeline runtime value type: " + value.getClass());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof PartitionValue)) {
      return false;
    }
    PartitionValue that = (PartitionValue) obj;
    if (_type != that._type) {
      return false;
    }
    if (_type == PartitionValueType.BYTES) {
      return Arrays.equals((byte[]) _value, (byte[]) that._value);
    }
    return Objects.equals(_value, that._value);
  }

  @Override
  public int hashCode() {
    return _type == PartitionValueType.BYTES ? 31 * _type.hashCode() + Arrays.hashCode((byte[]) _value)
        : 31 * _type.hashCode() + _value.hashCode();
  }
}
