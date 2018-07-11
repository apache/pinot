/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.HavingQueryTree;
import java.util.ArrayList;
import java.util.List;


/**
 * AST node for a list of predicates joined by boolean operators.
 */
public class PredicateListAstNode extends PredicateAstNode {

  public void buildQueryTreePredicates(List<FilterQueryTree> filterQueryOrPredicates,
      List<FilterQueryTree> filterQueryAndPredicates, List<HavingQueryTree> havingQueryOrPredicates,
      List<HavingQueryTree> havingQueryAndPredicates, boolean isItHaving) {
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
      if (!isItHaving) {
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
            ;
          } else {
            filterQueryOrPredicates.add(predicate.buildFilterQueryTree());
          }
        }
      } else {
        //Creating having Query tree is basically similar to creating filter query tree
        if (nextOperator == null) {
          if (!havingQueryAndPredicates.isEmpty()) {
            havingQueryAndPredicates.add(predicate.buildHavingQueryTree());
            if (!havingQueryOrPredicates.isEmpty()) {
              havingQueryOrPredicates.add(buildHavingPredicate(havingQueryAndPredicates, FilterOperator.AND));
            }
          } else {
            // Previous predicate was OR, therefore add the predicate directly
            havingQueryOrPredicates.add(predicate.buildHavingQueryTree());
          }
        } else if (nextOperator == BooleanOperatorAstNode.AND) {
          havingQueryAndPredicates.add(predicate.buildHavingQueryTree());
        } else {
          if (!havingQueryAndPredicates.isEmpty()) {
            havingQueryAndPredicates.add(predicate.buildHavingQueryTree());
            havingQueryOrPredicates.add(buildHavingPredicate(havingQueryAndPredicates, FilterOperator.AND));
            havingQueryAndPredicates = new ArrayList<>();
          } else {
            havingQueryOrPredicates.add(predicate.buildHavingQueryTree());
          }
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

    buildQueryTreePredicates(orPredicates, andPredicates, null, null, false);

    if (!orPredicates.isEmpty()) {
      return buildFilterPredicate(orPredicates, FilterOperator.OR);
    } else {
      return buildFilterPredicate(andPredicates, FilterOperator.AND);
    }
  }

  @Override
  public HavingQueryTree buildHavingQueryTree() {
    List<HavingQueryTree> orPredicates = new ArrayList<>();
    List<HavingQueryTree> andPredicates = new ArrayList<>();

    int childrenCount = getChildren().size();
    if (childrenCount == 1) {
      return ((PredicateAstNode) getChildren().get(0)).buildHavingQueryTree();
    }

    buildQueryTreePredicates(null, null, orPredicates, andPredicates, true);

    if (!orPredicates.isEmpty()) {
      return buildHavingPredicate(orPredicates, FilterOperator.OR);
    } else {
      return buildHavingPredicate(andPredicates, FilterOperator.AND);
    }
  }

  private FilterQueryTree buildFilterPredicate(List<FilterQueryTree> children, FilterOperator operator) {
    return new FilterQueryTree(null, null, operator, children);
  }

  private HavingQueryTree buildHavingPredicate(List<HavingQueryTree> children, FilterOperator operator) {
    return new HavingQueryTree(null, null, operator, children);
  }
}
