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
package org.apache.pinot.core.query.request.context;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.pinot.common.request.transform.TransformExpressionTree;


/**
 * The {@code ExpressionContext} class represents an expression in the query.
 * <p>The expression can be a LITERAL (e.g. 1, "abc"), an IDENTIFIER (e.g. memberId, timestamp), or a FUNCTION (e.g.
 * SUM(price), ADD(foo, bar)).
 * <p>Currently the query engine processes all literals as strings, so we store literals in string format (1 is stored
 * as "1").
 */
public class ExpressionContext {
  public enum Type {
    LITERAL, IDENTIFIER, FUNCTION
  }

  private final Type _type;
  private final String _value;
  private final FunctionContext _function;

  public static ExpressionContext forLiteral(String literal) {
    return new ExpressionContext(Type.LITERAL, literal, null);
  }

  public static ExpressionContext forIdentifier(String identifier) {
    return new ExpressionContext(Type.IDENTIFIER, identifier, null);
  }

  public static ExpressionContext forFunction(FunctionContext function) {
    return new ExpressionContext(Type.FUNCTION, null, function);
  }

  private ExpressionContext(Type type, String value, FunctionContext function) {
    _type = type;
    _value = value;
    _function = function;
  }

  public Type getType() {
    return _type;
  }

  public String getLiteral() {
    return _value;
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

  /**
   * Temporary helper method to help the migration from BrokerRequest to QueryContext.
   */
  public TransformExpressionTree toTransformExpressionTree() {
    switch (_type) {
      case LITERAL:
        return new TransformExpressionTree(TransformExpressionTree.ExpressionType.LITERAL, _value, null);
      case IDENTIFIER:
        return new TransformExpressionTree(TransformExpressionTree.ExpressionType.IDENTIFIER, _value, null);
      case FUNCTION:
        Preconditions.checkState(_function.getType() == FunctionContext.Type.TRANSFORM);
        List<ExpressionContext> arguments = _function.getArguments();
        List<TransformExpressionTree> children = new ArrayList<>(arguments.size());
        for (ExpressionContext argument : arguments) {
          children.add(argument.toTransformExpressionTree());
        }
        return new TransformExpressionTree(TransformExpressionTree.ExpressionType.FUNCTION, _function.getFunctionName(),
            children);
      default:
        throw new IllegalStateException();
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
    return _type == that._type && Objects.equals(_value, that._value) && Objects.equals(_function, that._function);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_type, _value, _function);
  }

  @Override
  public String toString() {
    switch (_type) {
      case LITERAL:
        return '\'' + _value + '\'';
      case IDENTIFIER:
        return _value;
      case FUNCTION:
        return _function.toString();
      default:
        throw new IllegalStateException();
    }
  }
}
