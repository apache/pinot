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
package org.apache.pinot.spi.systemtable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;


/**
 * Simple predicate tree representation for system table queries.
 * This keeps the SPI independent from broker request classes while still allowing basic push-down.
 */
public final class SystemTableFilter {
  public enum Operator {
    AND,
    OR,
    NOT,
    EQ,
    NEQ,
    GT,
    GTE,
    LT,
    LTE,
    IN,
    BETWEEN,
    REGEXP_LIKE
  }

  private final Operator _operator;
  private final @Nullable String _column;
  private final List<String> _values;
  private final List<SystemTableFilter> _children;

  private SystemTableFilter(Operator operator, @Nullable String column, List<String> values,
      List<SystemTableFilter> children) {
    _operator = Objects.requireNonNull(operator, "operator must not be null");
    _column = column;
    _values = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(values, "values must not be null")));
    _children =
        Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(children, "children must not be null")));
  }

  /**
   * Build a leaf predicate on a column.
   */
  public static SystemTableFilter predicate(String column, Operator operator, List<String> values) {
    return new SystemTableFilter(operator, Objects.requireNonNull(column, "column must not be null"), values,
        Collections.emptyList());
  }

  /**
   * Build a boolean node (AND/OR/NOT).
   */
  public static SystemTableFilter booleanOperator(Operator operator, List<SystemTableFilter> children) {
    return new SystemTableFilter(operator, null, Collections.emptyList(), children);
  }

  public Operator getOperator() {
    return _operator;
  }

  /**
   * Column name for leaf predicates. Null for boolean nodes.
   */
  public @Nullable String getColumn() {
    return _column;
  }

  /**
   * Predicate values (may be empty for boolean nodes or unary operators like NOT).
   */
  public List<String> getValues() {
    return _values;
  }

  /**
   * Children for boolean nodes. Empty for leaf predicates.
   */
  public List<SystemTableFilter> getChildren() {
    return _children;
  }
}
