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


/**
 * The {@code OrderByExpressionContext} class represents an expression in the ORDER-BY clause. It encapsulates an
 * expression and the expected ordering of the expression.
 */
public class OrderByExpressionContext {
  private final ExpressionContext _expression;
  private final boolean _isAsc;
  private final Boolean _isNullsLast;

  public OrderByExpressionContext(ExpressionContext expression, boolean isAsc) {
    _expression = expression;
    _isAsc = isAsc;
    _isNullsLast = null;
  }

  public OrderByExpressionContext(ExpressionContext expression, boolean isAsc, boolean isNullsLast) {
    _expression = expression;
    _isAsc = isAsc;
    _isNullsLast = isNullsLast;
  }

  public ExpressionContext getExpression() {
    return _expression;
  }

  public boolean isAsc() {
    return _isAsc;
  }

  public boolean isNullsLast() {
    // By default, null values sort as if larger than any non-null value; that is, NULLS FIRST is the default for DESC
    // order, and NULLS LAST otherwise.
    if (_isNullsLast == null) {
      return _isAsc;
    } else {
      return _isNullsLast;
    }
  }

  /**
   * Adds the columns (IDENTIFIER expressions) in the order-by expression to the given set.
   */
  public void getColumns(Set<String> columns) {
    _expression.getColumns(columns);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OrderByExpressionContext)) {
      return false;
    }
    OrderByExpressionContext that = (OrderByExpressionContext) o;
    return Objects.equals(_expression, that._expression) && _isAsc == that._isAsc && _isNullsLast == that._isNullsLast;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_expression, _isAsc, _isNullsLast);
  }

  @Override
  public String toString() {
    if (_isNullsLast != null) {
      return _expression.toString() + (_isAsc ? " ASC" : " DESC") + (_isNullsLast ? " NULLS LAST" : " NULLS FIRST");
    } else {
      return _expression.toString() + (_isAsc ? " ASC" : " DESC");
    }
  }
}
