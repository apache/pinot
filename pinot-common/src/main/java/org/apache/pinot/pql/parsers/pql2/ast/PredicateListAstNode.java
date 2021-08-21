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
package org.apache.pinot.pql.parsers.pql2.ast;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;


/**
 * AST node for a list of predicates joined by boolean operators.
 */
public class PredicateListAstNode extends PredicateAstNode {

  public void buildQueryTreePredicates(List<FilterQueryTree> filterQueryOrPredicates,
      List<FilterQueryTree> filterQueryAndPredicates) {
    int childrenCount = getChildren().size();
    for (int i = 0; i < childrenCount; i += 2) {
      PredicateAstNode predicate = (PredicateAstNode) getChildren().get(i);
      BooleanOperatorAstNode nextOperator = null;

      if (i + 1 < childrenCount) {
        nextOperator = (BooleanOperatorAstNode) getChildren().get(i + 1);
      }

      // 3 cases for the next operator:
      // - No next operator: Add the predicate to the AND predicates or to the parent OR predicate
      // - AND: Add the predicate to the AND predicates, creating it if necessary
      // - OR:
      //   1. Add the predicate to the AND predicates if it exists, then add it to the parent OR predicate
      //   2. If there is no current AND predicate list, add the predicate directly
      //   3. Clear the current AND predicates list
      // Is it the last predicate?
      if (nextOperator == null) {
        if (!filterQueryAndPredicates.isEmpty()) {
          filterQueryAndPredicates.add(predicate.buildFilterQueryTree());
          if (!filterQueryOrPredicates.isEmpty()) {
            filterQueryOrPredicates.add(buildFilterPredicate(filterQueryAndPredicates, FilterOperator.AND));
          }
        } else {
          // Previous predicate was OR, therefore add the predicate directly
          filterQueryOrPredicates.add(predicate.buildFilterQueryTree());
        }
      } else if (nextOperator == BooleanOperatorAstNode.AND) {
        filterQueryAndPredicates.add(predicate.buildFilterQueryTree());
      } else {
        if (!filterQueryAndPredicates.isEmpty()) {
          filterQueryAndPredicates.add(predicate.buildFilterQueryTree());
          filterQueryOrPredicates.add(buildFilterPredicate(filterQueryAndPredicates, FilterOperator.AND));
          filterQueryAndPredicates = new ArrayList<>();
        } else {
          filterQueryOrPredicates.add(predicate.buildFilterQueryTree());
        }
      }
    }
  }

  @Override
  public FilterQueryTree buildFilterQueryTree() {
    List<FilterQueryTree> orPredicates = new ArrayList<>();
    List<FilterQueryTree> andPredicates = new ArrayList<>();

    int childrenCount = getChildren().size();
    if (childrenCount == 1) {
      return ((PredicateAstNode) getChildren().get(0)).buildFilterQueryTree();
    }

    buildQueryTreePredicates(orPredicates, andPredicates);

    if (!orPredicates.isEmpty()) {
      return buildFilterPredicate(orPredicates, FilterOperator.OR);
    } else {
      return buildFilterPredicate(andPredicates, FilterOperator.AND);
    }
  }

  @Override
  public Expression buildFilterExpression() {

    List<Expression> filterQueryOrExpressions = new ArrayList<>();
    List<Expression> filterQueryAndExpressions = new ArrayList<>();

    int childrenCount = getChildren().size();
    if (childrenCount == 1) {
      return ((PredicateAstNode) getChildren().get(0)).buildFilterExpression();
    }

    for (int i = 0; i < childrenCount; i += 2) {
      PredicateAstNode predicate = (PredicateAstNode) getChildren().get(i);
      BooleanOperatorAstNode nextOperator = null;

      if (i + 1 < childrenCount) {
        nextOperator = (BooleanOperatorAstNode) getChildren().get(i + 1);
      }
      // 3 cases for the next operator:
      // - No next operator: Add the predicate to the AND predicates or to the parent OR predicate
      // - AND: Add the predicate to the AND predicates, creating it if necessary
      // - OR:
      //   1. Add the predicate to the AND predicates if it exists, then add it to the parent OR predicate
      //   2. If there is no current AND predicate list, add the predicate directly
      //   3. Clear the current AND predicates list
      // Is it the last predicate?
      if (nextOperator == null) {
        if (!filterQueryAndExpressions.isEmpty()) {
          filterQueryAndExpressions.add(predicate.buildFilterExpression());
          if (!filterQueryOrExpressions.isEmpty()) {
            filterQueryOrExpressions.add(buildFilterExpression(FilterKind.AND, filterQueryAndExpressions));
          }
        } else {
          // Previous predicate was OR, therefore add the predicate directly
          filterQueryOrExpressions.add(predicate.buildFilterExpression());
        }
      } else if (nextOperator == BooleanOperatorAstNode.AND) {
        filterQueryAndExpressions.add(predicate.buildFilterExpression());
      } else {
        if (!filterQueryAndExpressions.isEmpty()) {
          filterQueryAndExpressions.add(predicate.buildFilterExpression());
          filterQueryOrExpressions.add(buildFilterExpression(FilterKind.AND, filterQueryAndExpressions));
          filterQueryAndExpressions = new ArrayList<>();
        } else {
          filterQueryOrExpressions.add(predicate.buildFilterExpression());
        }
      }
    }

    if (!filterQueryOrExpressions.isEmpty()) {
      return buildFilterExpression(FilterKind.OR, filterQueryOrExpressions);
    } else {
      return buildFilterExpression(FilterKind.AND, filterQueryAndExpressions);
    }
  }

  public Expression buildFilterExpression(FilterKind operator, List<Expression> children) {
    final Expression expression = RequestUtils.getFunctionExpression(operator.name());
    for (Expression child : children) {
      expression.getFunctionCall().addToOperands(child);
    }
    return expression;
  }

  private FilterQueryTree buildFilterPredicate(List<FilterQueryTree> children, FilterOperator operator) {
    return new FilterQueryTree(null, null, operator, children);
  }
}
