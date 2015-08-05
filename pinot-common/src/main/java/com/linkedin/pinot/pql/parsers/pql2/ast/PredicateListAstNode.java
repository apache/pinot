/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.pql.parsers.pql2.ast;

import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import java.util.ArrayList;
import java.util.List;


/**
 * AST node for a list of predicates joined by boolean operators.
 */
public class PredicateListAstNode extends PredicateAstNode {
  @Override
  public FilterQueryTree buildFilterQueryTree() {
    List<FilterQueryTree> orPredicates = null;
    List<FilterQueryTree> andPredicates = null;

    int childrenCount = getChildren().size();

    if (childrenCount == 1) {
      return ((PredicateAstNode)getChildren().get(0)).buildFilterQueryTree();
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
        if (andPredicates != null) {
          andPredicates.add(predicate.buildFilterQueryTree());

          if (orPredicates != null) {
            orPredicates.add(buildPredicate(andPredicates, FilterOperator.AND));
          }
        } else {
          // Previous predicate was OR, therefore add the predicate directly
          orPredicates.add(predicate.buildFilterQueryTree());
        }
      } else if (nextOperator == BooleanOperatorAstNode.AND) {
        if (andPredicates == null) {
          andPredicates = new ArrayList<>();
        }

        andPredicates.add(predicate.buildFilterQueryTree());
      } else {
        if (orPredicates == null) {
          orPredicates = new ArrayList<>();
        }

        if (andPredicates != null) {
          andPredicates.add(predicate.buildFilterQueryTree());
          orPredicates.add(buildPredicate(andPredicates, FilterOperator.AND));
          andPredicates = null;
        } else {
          orPredicates.add(predicate.buildFilterQueryTree());
        }
      }
    }

    if (orPredicates != null) {
      return buildPredicate(orPredicates, FilterOperator.OR);
    } else {
      return buildPredicate(andPredicates, FilterOperator.AND);
    }
  }

  private FilterQueryTree buildPredicate(List<FilterQueryTree> children, FilterOperator operator) {
    return new FilterQueryTree(null, null, operator, children);
  }
}
