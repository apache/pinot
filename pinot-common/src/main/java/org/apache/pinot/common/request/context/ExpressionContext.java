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

import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * The {@code ExpressionContext} class represents an expression in the query.
 * <p>The expression can be a LITERAL (e.g. 1, "abc"), an IDENTIFIER (e.g. memberId, timestamp), or a FUNCTION (e.g.
 * SUM(price), ADD(foo, bar)).
 * <p>Currently the query engine processes all literals as strings, so we store literals in string format (1 is stored
 * as "1").
 */
public class ExpressionContext {
  public enum Type {
    IDENTIFIER, FUNCTION, LITERAL_CONTEXT
  }

  private final Type _type;
  private final String _value;
  private final FunctionContext _function;
  // Only set when the _type is LITERAL_CONTEXT
  @Nullable
  private final LiteralContext _literal;

  public static ExpressionContext forLiteralContext(Literal literal){
    return new ExpressionContext(Type.LITERAL_CONTEXT, null, null, new LiteralContext(literal));
  }
  public static ExpressionContext forLiteralContext(FieldSpec.DataType type, Object val){
    return new ExpressionContext(Type.LITERAL_CONTEXT, null, null, new LiteralContext(type, val));
  }

  public static ExpressionContext forIdentifier(String identifier) {
    return new ExpressionContext(Type.IDENTIFIER, identifier, null, null);
  }

  public static ExpressionContext forFunction(FunctionContext function) {
    return new ExpressionContext(Type.FUNCTION, null, function, null);
  }

  private ExpressionContext(Type type, String value, FunctionContext function, LiteralContext literal) {
    _type = type;
    _value = value;
    _function = function;
    _literal = literal;
  }

  // TODO: Deprecate the usage of literal string.
  public String getLiteralString() {
    if (_literal == null || _literal.getValue() == null) {
      return "";
    }
    return _literal.getValue().toString();
  }

  public Type getType() {
    return _type;
  }

  @Nullable
  public LiteralContext getLiteralContext(){
    return _literal;
  }

  public String getIdentifier() {
    return _value;
  }

  public FunctionContext getFunction() {
    return _function;
  }

  /**
   * Adds the columns (IDENTIFIER expressions) in the expression to the given set.
   */
  public void getColumns(Set<String> columns) {
    if (_type == Type.IDENTIFIER) {
      if (!_value.equals("*")) {
        columns.add(_value);
      }
    } else if (_type == Type.FUNCTION) {
      _function.getColumns(columns);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExpressionContext)) {
      return false;
    }
    ExpressionContext that = (ExpressionContext) o;
    return _type.equals(that._type) && Objects.equals(_value, that._value) && Objects.equals(_function, that._function) && Objects.equals(_literal, that._literal);
  }

  @Override
  public int hashCode() {
    int hash = 31 * 31 * _type.hashCode();
    if (_type == Type.FUNCTION) {
      return hash + _function.hashCode();
    }
    if (_type == Type.LITERAL_CONTEXT) {
      return hash + _literal.hashCode();
    }
    return hash + 31 * _value.hashCode();
  }

  @Override
  public String toString() {
    switch (_type) {
      case LITERAL_CONTEXT:
        return _literal.toString();
      case IDENTIFIER:
        return _value;
      case FUNCTION:
        return _function.toString();
      default:
        throw new IllegalStateException();
    }
  }
}
