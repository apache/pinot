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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.utils.PinotDataType;
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

  public LiteralContext(DataType type, Object value) {
    _type = type;
    _value = value;
    _pinotDataType = getPinotDataType(type);
  }

  public LiteralContext(Literal literal) {
    switch (literal.getSetField()) {
      case BOOL_VALUE:
        _type = DataType.BOOLEAN;
        _value = literal.getBoolValue();
        break;
      case INT_VALUE:
        _type = DataType.INT;
        _value = literal.getIntValue();
        break;
      case LONG_VALUE:
        // TODO: Remove this special handling after broker uses INT type for integer literals.
        //       https://github.com/apache/pinot/pull/11751
//        _type = DataType.LONG;
//        _value = literal.getLongValue();
        long longValue = literal.getLongValue();
        if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
          _type = DataType.INT;
          _value = (int) longValue;
        } else {
          _type = DataType.LONG;
          _value = longValue;
        }
        break;
      case FLOAT_VALUE:
        _type = DataType.FLOAT;
        _value = Float.intBitsToFloat(literal.getFloatValue());
        break;
      case DOUBLE_VALUE:
        _type = DataType.DOUBLE;
        _value = literal.getDoubleValue();
        break;
      case BIG_DECIMAL_VALUE:
        _type = DataType.BIG_DECIMAL;
        _value = BigDecimalUtils.deserialize(literal.getBigDecimalValue());
        break;
      case STRING_VALUE:
        _type = DataType.STRING;
        _value = literal.getStringValue();
        break;
      case BINARY_VALUE:
        _type = DataType.BYTES;
        _value = literal.getBinaryValue();
        break;
      case INT_ARRAY_VALUE:
        _type = DataType.INT;
        _value = literal.getIntArrayValue();
        break;
      case LONG_ARRAY_VALUE:
        _type = DataType.LONG;
        _value = literal.getLongArrayValue();
        break;
      case FLOAT_ARRAY_VALUE:
        _type = DataType.FLOAT;
        _value = literal.getFloatArrayValue();
        break;
      case DOUBLE_ARRAY_VALUE:
        _type = DataType.DOUBLE;
        _value = literal.getDoubleArrayValue();
        break;
      case STRING_ARRAY_VALUE:
        _type = DataType.STRING;
        _value = literal.getStringArrayValue();
        break;
      case NULL_VALUE:
        _type = DataType.UNKNOWN;
        _value = null;
        break;
      default:
        throw new IllegalStateException("Unsupported field type: " + literal.getSetField());
    }
    _pinotDataType = getPinotDataType(_type);
  }

  private static PinotDataType getPinotDataType(DataType type) {
    switch (type) {
      case BOOLEAN:
        return PinotDataType.BOOLEAN;
      case INT:
        return PinotDataType.INTEGER;
      case LONG:
        return PinotDataType.LONG;
      case FLOAT:
        return PinotDataType.FLOAT;
      case DOUBLE:
        return PinotDataType.DOUBLE;
      case BIG_DECIMAL:
        return PinotDataType.BIG_DECIMAL;
      case STRING:
        return PinotDataType.STRING;
      case BYTES:
        return PinotDataType.BYTES;
      case UNKNOWN:
        return null;
      default:
        throw new IllegalStateException("Unsupported data type: " + type);
    }
  }

  public DataType getType() {
    return _type;
  }

  @Nullable
  public Object getValue() {
    return _value;
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
        // Pinot uses long to represent TIMESTAMP value, so we need to handle the case when the value is the string
        // representation of a TIMESTAMP value.
        try {
          longValue = Timestamp.valueOf((String) _value).getTime();
        } catch (IllegalArgumentException e1) {
          throw new IllegalArgumentException("Invalid long value: " + _value);
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
    } else {
      return "'" + getStringValue() + "'";
    }
  }
}
