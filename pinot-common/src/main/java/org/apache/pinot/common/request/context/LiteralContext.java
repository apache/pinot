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
package org.apache.pinot.common.request.context;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;


/**
 * The {@code LiteralContext} class represents a literal in the query.
 * <p>This includes both value and type information. We translate thrift literal to this representation in server.
 */
public class LiteralContext {
  // TODO: Support all of the types for sql.
  private final DataType _type;
  private final Object _value;

  /**
   * This is used for type conversion, and is not included in {@link #equals} and {@link #hashCode}.
   */
  private final PinotDataType _pinotDataType;

  private Boolean _booleanValue;
  private Integer _intValue;
  private Long _longValue;
  private Float _floatValue;
  private Double _doubleValue;
  private BigDecimal _bigDecimalValue;
  private String _stringValue;
  private byte[] _bytesValue;

  public LiteralContext(Literal literal) {
    switch (literal.getSetField()) {
      case NULL_VALUE:
        _type = DataType.UNKNOWN;
        _value = null;
        _pinotDataType = null;
        break;
      case BOOL_VALUE:
        _type = DataType.BOOLEAN;
        _value = literal.getBoolValue();
        _pinotDataType = PinotDataType.BOOLEAN;
        break;
      case INT_VALUE:
        _type = DataType.INT;
        _value = literal.getIntValue();
        _pinotDataType = PinotDataType.INTEGER;
        break;
      case LONG_VALUE:
        _type = DataType.LONG;
        _value = literal.getLongValue();
        _pinotDataType = PinotDataType.LONG;
        break;
      case FLOAT_VALUE:
        _type = DataType.FLOAT;
        _value = Float.intBitsToFloat(literal.getFloatValue());
        _pinotDataType = PinotDataType.FLOAT;
        break;
      case DOUBLE_VALUE:
        _type = DataType.DOUBLE;
        _value = literal.getDoubleValue();
        _pinotDataType = PinotDataType.DOUBLE;
        break;
      case BIG_DECIMAL_VALUE:
        _type = DataType.BIG_DECIMAL;
        _value = BigDecimalUtils.deserialize(literal.getBigDecimalValue());
        _pinotDataType = PinotDataType.BIG_DECIMAL;
        break;
      case STRING_VALUE:
        _type = DataType.STRING;
        _value = literal.getStringValue();
        _pinotDataType = PinotDataType.STRING;
        break;
      case BINARY_VALUE:
        _type = DataType.BYTES;
        _value = literal.getBinaryValue();
        _pinotDataType = PinotDataType.BYTES;
        break;
      case INT_ARRAY_VALUE: {
        _type = DataType.INT;
        _value = RequestUtils.getIntArrayValue(literal);
        _pinotDataType = PinotDataType.PRIMITIVE_INT_ARRAY;
        break;
      }
      case LONG_ARRAY_VALUE: {
        _type = DataType.LONG;
        _value = RequestUtils.getLongArrayValue(literal);
        _pinotDataType = PinotDataType.PRIMITIVE_LONG_ARRAY;
        break;
      }
      case FLOAT_ARRAY_VALUE: {
        _type = DataType.FLOAT;
        _value = RequestUtils.getFloatArrayValue(literal);
        _pinotDataType = PinotDataType.PRIMITIVE_FLOAT_ARRAY;
        break;
      }
      case DOUBLE_ARRAY_VALUE: {
        _type = DataType.DOUBLE;
        _value = RequestUtils.getDoubleArrayValue(literal);
        _pinotDataType = PinotDataType.PRIMITIVE_DOUBLE_ARRAY;
        break;
      }
      case STRING_ARRAY_VALUE:
        _type = DataType.STRING;
        _value = RequestUtils.getStringArrayValue(literal);
        _pinotDataType = PinotDataType.STRING_ARRAY;
        break;
      default:
        throw new IllegalStateException("Unsupported field type: " + literal.getSetField());
    }
  }

  @VisibleForTesting
  public LiteralContext(DataType type, @Nullable Object value) {
    _type = type;
    _value = value;
    _pinotDataType = getPinotDataType(type, value);
  }

  @Nullable
  private static PinotDataType getPinotDataType(DataType type, @Nullable Object value) {
    if (value == null) {
      return null;
    }
    if (type == DataType.BYTES) {
      Preconditions.checkState(value.getClass().getComponentType() == byte.class, "Bytes array is not supported");
      return PinotDataType.BYTES;
    }
    boolean singleValue = !value.getClass().isArray();
    switch (type) {
      case BOOLEAN:
        Preconditions.checkState(singleValue, "Boolean array is not supported");
        return PinotDataType.BOOLEAN;
      case INT:
        return singleValue ? PinotDataType.INTEGER : PinotDataType.PRIMITIVE_INT_ARRAY;
      case LONG:
        return singleValue ? PinotDataType.LONG : PinotDataType.PRIMITIVE_LONG_ARRAY;
      case FLOAT:
        return singleValue ? PinotDataType.FLOAT : PinotDataType.PRIMITIVE_FLOAT_ARRAY;
      case DOUBLE:
        return singleValue ? PinotDataType.DOUBLE : PinotDataType.PRIMITIVE_DOUBLE_ARRAY;
      case BIG_DECIMAL:
        Preconditions.checkState(singleValue, "BigDecimal array is not supported");
        return PinotDataType.BIG_DECIMAL;
      case STRING:
        return singleValue ? PinotDataType.STRING : PinotDataType.STRING_ARRAY;
      default:
        throw new IllegalStateException("Unsupported DataType: " + type);
    }
  }

  public DataType getType() {
    return _type;
  }

  @Nullable
  public Object getValue() {
    return _value;
  }

  public boolean isSingleValue() {
    return _pinotDataType == null || _pinotDataType.isSingleValue();
  }

  public boolean getBooleanValue() {
    Boolean booleanValue = _booleanValue;
    if (booleanValue == null) {
      booleanValue = _pinotDataType != null && _pinotDataType.toBoolean(_value);
      _booleanValue = booleanValue;
    }
    return booleanValue;
  }

  public int getIntValue() {
    Integer intValue = _intValue;
    if (intValue == null) {
      try {
        intValue = _pinotDataType != null ? _pinotDataType.toInt(_value) : NullValuePlaceHolder.INT;
      } catch (NumberFormatException e) {
        // Pinot uses int to represent BOOLEAN value, so we need to handle the case when the value is the string
        // representation of a BOOLEAN value.
        String stringValue = (String) _value;
        if (stringValue.equalsIgnoreCase("true")) {
          intValue = 1;
        } else if (stringValue.equalsIgnoreCase("false")) {
          intValue = 0;
        } else {
          throw new IllegalArgumentException("Invalid int value: " + _value);
        }
      }
      _intValue = intValue;
    }
    return intValue;
  }

  public long getLongValue() {
    Long longValue = _longValue;
    if (longValue == null) {
      try {
        longValue = _pinotDataType != null ? _pinotDataType.toLong(_value) : NullValuePlaceHolder.LONG;
      } catch (NumberFormatException e) {
        // Pinot uses long to represent TIMESTAMP or TIMESTAMP_NTZ value, so we need to handle the case
        // when the value is the string representation of a TIMESTAMP or TIMESTAMP_NTZ value.
        try {
          longValue = Timestamp.valueOf((String) _value).getTime();
        } catch (IllegalArgumentException e1) {
          try {
            longValue = LocalDateTime.parse((String) _value).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
          } catch (Exception e2) {
            throw new IllegalArgumentException("Invalid long value: " + _value);
          }
        }
      }
      _longValue = longValue;
    }
    return longValue;
  }

  public float getFloatValue() {
    Float floatValue = _floatValue;
    if (floatValue == null) {
      floatValue = _pinotDataType != null ? _pinotDataType.toFloat(_value) : NullValuePlaceHolder.FLOAT;
      _floatValue = floatValue;
    }
    return floatValue;
  }

  public double getDoubleValue() {
    Double doubleValue = _doubleValue;
    if (doubleValue == null) {
      doubleValue = _pinotDataType != null ? _pinotDataType.toDouble(_value) : NullValuePlaceHolder.DOUBLE;
      _doubleValue = doubleValue;
    }
    return doubleValue;
  }

  public BigDecimal getBigDecimalValue() {
    BigDecimal bigDecimalValue = _bigDecimalValue;
    if (bigDecimalValue == null) {
      bigDecimalValue = _pinotDataType != null ? _pinotDataType.toBigDecimal(_value) : NullValuePlaceHolder.BIG_DECIMAL;
      _bigDecimalValue = bigDecimalValue;
    }
    return bigDecimalValue;
  }

  public String getStringValue() {
    String stringValue = _stringValue;
    if (stringValue == null) {
      stringValue = _pinotDataType != null ? _pinotDataType.toString(_value) : NullValuePlaceHolder.STRING;
      _stringValue = stringValue;
    }
    return stringValue;
  }

  public byte[] getBytesValue() {
    byte[] bytesValue = _bytesValue;
    if (bytesValue == null) {
      bytesValue = _pinotDataType != null ? _pinotDataType.toBytes(_value) : NullValuePlaceHolder.BYTES;
      _bytesValue = bytesValue;
    }
    return bytesValue;
  }

  public boolean isNull() {
    return _value == null;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_value, _type);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LiteralContext)) {
      return false;
    }
    LiteralContext that = (LiteralContext) o;
    return _type.equals(that._type) && Objects.equals(_value, that._value);
  }

  @Override
  public String toString() {
    // TODO: Remove single quotes except for string and bytes after broker reducer supports expression format change.
    //       https://github.com/apache/pinot/pull/11762)
    if (isNull()) {
      return "'null'";
    }
    if (isSingleValue()) {
      return "'" + getStringValue() + "'";
    }
    switch (_pinotDataType) {
      case PRIMITIVE_INT_ARRAY:
        return "'" + Arrays.toString((int[]) _value) + "'";
      case PRIMITIVE_LONG_ARRAY:
        return "'" + Arrays.toString((long[]) _value) + "'";
      case PRIMITIVE_FLOAT_ARRAY:
        return "'" + Arrays.toString((float[]) _value) + "'";
      case PRIMITIVE_DOUBLE_ARRAY:
        return "'" + Arrays.toString((double[]) _value) + "'";
      default:
        throw new IllegalStateException("Unsupported PinotDataType: " + _pinotDataType);
    }
  }
}
