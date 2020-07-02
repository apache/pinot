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
package org.apache.pinot.core.common;

import java.util.List;
import java.util.Objects;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.core.common.predicate.EqPredicate;
import org.apache.pinot.core.common.predicate.InPredicate;
import org.apache.pinot.core.common.predicate.IsNotNullPredicate;
import org.apache.pinot.core.common.predicate.IsNullPredicate;
import org.apache.pinot.core.common.predicate.NEqPredicate;
import org.apache.pinot.core.common.predicate.NotInPredicate;
import org.apache.pinot.core.common.predicate.RangePredicate;
import org.apache.pinot.core.common.predicate.RegexpLikePredicate;
import org.apache.pinot.core.common.predicate.TextMatchPredicate;


public abstract class Predicate {

  public enum Type {
    EQ, NEQ, REGEXP_LIKE, RANGE, IN, NOT_IN, IS_NULL, IS_NOT_NULL, TEXT_MATCH;

    public boolean isExclusive() {
      return this == NEQ || this == NOT_IN;
    }
  }

  private final String lhs;
  private final List<String> rhs;
  private final Type type;

  public Predicate(String lhs, Type predicateType, List<String> rhs) {
    this.lhs = lhs;
    type = predicateType;
    this.rhs = rhs;
  }

  protected String getLhs() {
    return lhs;
  }

  protected List<String> getRhs() {
    return rhs;
  }

  public Type getType() {
    return type;
  }

  @Override
  public String toString() {
    return "Predicate: type: " + type + ", left: " + lhs + ", right: " + rhs;
  }

  public static Predicate newPredicate(FilterQueryTree filterQueryTree) {
    assert (filterQueryTree.getChildren() == null) || filterQueryTree.getChildren().isEmpty();
    final FilterOperator filterType = filterQueryTree.getOperator();
    final String column = filterQueryTree.getColumn();
    final List<String> value = filterQueryTree.getValue();

    Predicate predicate = newPredicate(filterType, column, value);
    return predicate;
  }

  public static Predicate newPredicate(FilterOperator filterType, String column, List<String> value) {
    Predicate predicate = null;
    switch (filterType) {
      case EQUALITY:
        predicate = new EqPredicate(column, value);
        break;
      case RANGE:
        predicate = new RangePredicate(column, value);
        break;
      case REGEXP_LIKE:
        predicate = new RegexpLikePredicate(column, value);
        break;
      case NOT:
        predicate = new NEqPredicate(column, value);
        break;
      case NOT_IN:
        predicate = new NotInPredicate(column, value);
        break;
      case IN:
        predicate = new InPredicate(column, value);
        break;
      case IS_NULL:
        predicate = new IsNullPredicate(column);
        break;
      case IS_NOT_NULL:
        predicate = new IsNotNullPredicate(column);
        break;
      case TEXT_MATCH:
        predicate = new TextMatchPredicate(column, value);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported filterType:" + filterType);
    }
    return predicate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Predicate)) {
      return false;
    }
    Predicate predicate = (Predicate) o;
    return Objects.equals(lhs, predicate.lhs) && Objects.equals(rhs, predicate.rhs) && type == predicate.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(lhs, rhs, type);
  }
}
