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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.Predicate;


/**
 * The {@code FilterContext} class encapsulates the information of a filter in the query. Both WHERE clause and HAVING
 * clause are modeled as a filter.
 */
public final class FilterContext {
  public static final FilterContext CONSTANT_TRUE = new FilterContext(Type.CONSTANT, null, null, true);
  public static final FilterContext CONSTANT_FALSE = new FilterContext(Type.CONSTANT, null, null, false);

  public enum Type {
    AND, OR, NOT, PREDICATE, CONSTANT
  }

  private final Type _type;

  // For AND, OR, NOT
  private final List<FilterContext> _children;

  // For Predicate
  private final Predicate _predicate;

  // For Constant
  private final boolean _isTrue;

  private FilterContext(Type type, @Nullable List<FilterContext> children, @Nullable Predicate predicate,
      boolean isTrue) {
    _type = type;
    _children = children;
    _predicate = predicate;
    _isTrue = isTrue;
  }

  public static FilterContext forAnd(List<FilterContext> children) {
    return new FilterContext(Type.AND, children, null, false);
  }

  public static FilterContext forOr(List<FilterContext> children) {
    return new FilterContext(Type.OR, children, null, false);
  }

  public static FilterContext forNot(FilterContext child) {
    return new FilterContext(Type.NOT, Collections.singletonList(child), null, false);
  }

  public static FilterContext forPredicate(Predicate predicate) {
    return new FilterContext(Type.PREDICATE, null, predicate, false);
  }

  public static FilterContext forConstant(boolean isTrue) {
    return isTrue ? CONSTANT_TRUE : CONSTANT_FALSE;
  }

  public Type getType() {
    return _type;
  }

  public List<FilterContext> getChildren() {
    return _children;
  }

  public Predicate getPredicate() {
    return _predicate;
  }

  public boolean isConstant() {
    return _type == Type.CONSTANT;
  }

  public boolean isConstantTrue() {
    return _isTrue;
  }

  public boolean isConstantFalse() {
    return _type == Type.CONSTANT && !_isTrue;
  }

  /**
   * Adds the columns (IDENTIFIER expressions) in the filter to the given set.
   */
  public void getColumns(Set<String> columns) {
    if (_children != null) {
      for (FilterContext child : _children) {
        child.getColumns(columns);
      }
    } else if (_predicate != null) {
      _predicate.getLhs().getColumns(columns);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FilterContext)) {
      return false;
    }
    FilterContext that = (FilterContext) o;
    return _isTrue == that._isTrue && _type == that._type && Objects.equals(_children, that._children)
        && Objects.equals(_predicate, that._predicate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_type, _children, _predicate, _isTrue);
  }

  @Override
  public String toString() {
    switch (_type) {
      case AND: {
        assert _children != null;
        StringBuilder stringBuilder = new StringBuilder().append('(').append(_children.get(0));
        int numChildren = _children.size();
        for (int i = 1; i < numChildren; i++) {
          stringBuilder.append(" AND ").append(_children.get(i));
        }
        return stringBuilder.append(')').toString();
      }
      case OR: {
        assert _children != null;
        StringBuilder stringBuilder = new StringBuilder().append('(').append(_children.get(0));
        int numChildren = _children.size();
        for (int i = 1; i < numChildren; i++) {
          stringBuilder.append(" OR ").append(_children.get(i));
        }
        return stringBuilder.append(')').toString();
      }
      case NOT:
        assert _children != null && _children.size() == 1;
        return "(NOT " + _children.get(0) + ')';
      case PREDICATE:
        assert _predicate != null;
        return _predicate.toString();
      case CONSTANT:
        return Boolean.toString(_isTrue);
      default:
        throw new IllegalStateException();
    }
  }
}
