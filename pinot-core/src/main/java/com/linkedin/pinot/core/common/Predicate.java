/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.common;

import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.common.predicate.RegexpLikePredicate;
import java.util.Arrays;
import java.util.List;


public abstract class Predicate {

  public enum Type {
    EQ,
    NEQ,
    REGEXP_LIKE,
    RANGE,
    IN,
    NOT_IN;

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
    return "Predicate: type: " + type + ", left : " + lhs + ", right : " + Arrays.toString(rhs.toArray(new String[0]))
        + "\n";
  }

  public static Predicate newPredicate(FilterQueryTree filterQueryTree) {
    assert (filterQueryTree.getChildren() == null) || filterQueryTree.getChildren().isEmpty();
    final FilterOperator filterType = filterQueryTree.getOperator();
    final String column = filterQueryTree.getColumn();
    final List<String> value = filterQueryTree.getValue();

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
    default:
      throw new UnsupportedOperationException("Unsupported filterType:" + filterType);
    }
    return predicate;
  }
}
