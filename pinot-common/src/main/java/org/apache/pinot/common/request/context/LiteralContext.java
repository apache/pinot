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

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * The {@code LiteralContext} class represents a literal in the query.
 * <p>This includes both value and type information. We translate thrift literal to this representation in server.
 * Currently, only Boolean literal is correctly encoded in thrift and passed in.
 * All integers are encoded as LONG in thrift, and the other numerical types are encoded as DOUBLE.
 * The remaining types are encoded as STRING.
 */
public class LiteralContext {
  // TODO: Support all of the types for sql.
  private FieldSpec.DataType _type;
  private Object _value;

  // TODO: Support all data types.
  private static FieldSpec.DataType convertThriftTypeToDataType(Literal._Fields fields) {
    switch (fields) {
      case LONG_VALUE:
        return FieldSpec.DataType.LONG;
      case BOOL_VALUE:
        return FieldSpec.DataType.BOOLEAN;
      case DOUBLE_VALUE:
        return FieldSpec.DataType.DOUBLE;
      case STRING_VALUE:
        return FieldSpec.DataType.STRING;
      default:
        throw new UnsupportedOperationException("Unsupported literal type:" + fields);
    }
  }

  private static Class<?> convertDataTypeToJavaType(FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return Integer.class;
      case LONG:
        return Long.class;
      case BOOLEAN:
        return Boolean.class;
      case FLOAT:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case STRING:
        return String.class;
      default:
        throw new UnsupportedOperationException("Unsupported dataType:" + dataType);
    }
  }

  public LiteralContext(Literal literal) {
    _type = convertThriftTypeToDataType(literal.getSetField());
    _value = literal.getFieldValue();
  }

  public FieldSpec.DataType getType() {
    return _type;
  }

  @Nullable
  public Object getValue() {
    return _value;
  }

  public LiteralContext(FieldSpec.DataType type, Object value) {
    Preconditions.checkArgument(convertDataTypeToJavaType(type) == value.getClass(),
        "Unmatched data type: " + type + " java type: " + value.getClass());
    _type = type;
    _value = value;
  }

  @Override
  public int hashCode() {
    if (_value == null) {
      return 31 * _type.hashCode();
    }
    return 31 * _value.hashCode();
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
    if (_value == null) {
      return "null";
    }
    // TODO: print out the type.
    return '\'' + _value.toString() + '\'';
  }
}
