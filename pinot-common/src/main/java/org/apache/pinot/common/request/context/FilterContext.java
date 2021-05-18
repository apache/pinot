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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.pinot.common.request.context.predicate.Predicate;


/**
 * The {@code FilterContext} class encapsulates the information of a filter in the query. Both WHERE clause and HAVING
 * clause are modeled as a filter.
 */
public class FilterContext {
  public enum Type {
    AND, OR, PREDICATE
  }

  private final Type _type;

  // For AND and OR
  private final List<FilterContext> _children;

  // For Predicate
  private final Predicate _predicate;

  public FilterContext(Type type, List<FilterContext> children, Predicate predicate) {
    _type = type;
    _children = children;
    _predicate = predicate;
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

  /**
   * Adds the columns (IDENTIFIER expressions) in the filter to the given set.
   */
  public void getColumns(Set<String> columns) {
    if (_children != null) {
      for (FilterContext child : _children) {
        child.getColumns(columns);
      }
    } else {
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
    return _type == that._type && Objects.equals(_children, that._children) && Objects
        .equals(_predicate, that._predicate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_type, _children, _predicate);
  }

  @Override
  public String toString() {
    switch (_type) {
      case AND:
        StringBuilder stringBuilder = new StringBuilder().append('(').append(_children.get(0).toString());
        int numChildren = _children.size();
        for (int i = 1; i < numChildren; i++) {
          stringBuilder.append(" AND ").append(_children.get(i).toString());
        }
        return stringBuilder.append(')').toString();
      case OR:
        stringBuilder = new StringBuilder().append('(').append(_children.get(0).toString());
        numChildren = _children.size();
        for (int i = 1; i < numChildren; i++) {
          stringBuilder.append(" OR ").append(_children.get(i).toString());
        }
        return stringBuilder.append(')').toString();
      case PREDICATE:
        return _predicate.toString();
      default:
        throw new IllegalStateException();
    }
  }
}
