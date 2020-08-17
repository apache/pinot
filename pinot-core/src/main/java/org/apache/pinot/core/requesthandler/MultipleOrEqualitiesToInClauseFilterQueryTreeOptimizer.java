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
package org.apache.pinot.core.requesthandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.utils.request.FilterQueryTree;


/**
 * The optimizer looks at the FilterQueryTree to convert
 *
 * (1) multiple OR equality predicates into a single IN predicate
 * e.g COL = 1 OR COl = 2 OR COL = 3 is rewritten as COL IN (1, 2, 3)
 *
 * (2) single value IN predicate into equality predicate
 * e.g COL IN (1) is rewritten as COL = 1
 *
 * (3) Deduplicates the predicate values for IN.
 *
 * These rewrites are also done for child subtrees (handling nested nature)
 *
 * NOTE: This optimizer is dependent on {@link FlattenNestedPredicatesFilterQueryTreeOptimizer}
 * first flattening the tree and removing redundant operators.
 */
public class MultipleOrEqualitiesToInClauseFilterQueryTreeOptimizer extends FilterQueryTreeOptimizer {

  @Override
  public FilterQueryTree optimize(FilterQueryOptimizerRequest request) {
    return optimize(request.getFilterQueryTree());
  }

  /**
   * Optimize a FilterQueryTree. When we look at the root operator,
   * there are 3 possible cases:
   *
   * (1) root is OR
   * (2) root is AND
   * (3) root itself is a leaf level operator: EQUALITY, RANGE, IN etc
   *
   * For (1) and (2), we look at the subtrees (children). For (3), currently
   * the only case where rewrite is needed is for single predicate IN
   *
   * @param filterQueryTree root of tree to optimize/rewrite
   * @return root of optimized tree
   */
  private FilterQueryTree optimize(FilterQueryTree filterQueryTree) {
    if (filterQueryTree.getOperator() == FilterOperator.OR) {
      // CASE 1: Root operator is OR
      return optimizeTreeRootedAtOR(filterQueryTree);
    } else if (filterQueryTree.getOperator() == FilterOperator.AND) {
      // CASE 2: Root operator is AND
      return optimizeTreeRootedAtAND(filterQueryTree);
    } else {
      // CASE 3:
      // if we are here, it means the query has a single predicate
      // (just one column with one or more predicate values)
      // a = 1
      // a IN (1)
      // a > 7
      // a IN (1, 2, 3, 4)
      // .... anything single predicate
      if (filterQueryTree.getOperator() == FilterOperator.IN) {
        // for single predicate rewriting, we currently handle the following scenario:
        // for IN , dedup the values and rewrite it as IN/EQ based on
        // whether the number of predicate values is more than 1 or 1
        List<String> values = filterQueryTree.getValue();
        if (values.size() > 1) {
          // if more than 1 value, dedup them
          // rewrite as IN if more than 1 value after deduping
          // rewrite as EQ if 1 value after deduping
          Set<String> deduped = new HashSet<>(values);
          values = new ArrayList<>(deduped);
          if (deduped.size() > 1) {
            // e.g a IN (1, 2, 3, 4, 2, 3) -> a IN (1, 2, 3, 4)
            return new FilterQueryTree(filterQueryTree.getColumn(), values, FilterOperator.IN, null);
          }
          // e.g a IN (1, 1, 1) -> a = 1
          return new FilterQueryTree(filterQueryTree.getColumn(), values, FilterOperator.EQUALITY, null);
        } else {
          // e.g a IN (1) -> a = 1
          return new FilterQueryTree(filterQueryTree.getColumn(), values, FilterOperator.EQUALITY, null);
        }
      }
      return filterQueryTree;
    }
  }

  /**
   * Optimize FilterQueryTree rooted at OR. We look at the immediate children
   * of OR from two different scenarios:
   *
   * (1) The immediate child could be a IN or an EQUALITY operator
   * (2) The immediate child could be other operators -- AND, NOT, RANGE, BETWEEN etc
   *
   * We first go over the immediate children and collect the operators
   * in two separate data structures.
   *
   * (1) is maintained in a map of column name to unique set of 1 or more
   * predicate values. For this we also track in a local variable whether
   * the predicates in the map need to be rewritten or not. This decision
   * is made by {@link #collectChildOperatorsOfRootOROperator(FilterQueryTree,
   * Map, List)} method as it builds the data structure
   *
   * (2) is maintained in a simple list of FilterQueryTrees representing the
   * non EQUALITY/IN operators. Also, as {@link #collectChildOperatorsOfRootOROperator
   * (FilterQueryTree, Map, List)} builds this list, it also optimizes these set of
   * FilterQueryTrees.
   *
   * We first deal with a very special case where the entire filter just has one column
   * and there aren't any non EQUALITY/IN operators. Few examples to represent this
   * scenario:
   *
   * A = 100 OR A = 200 OR A = 300
   * A IN (100) OR A IN (200) OR A IN (300)
   * A = 100 OR A = 200 OR A = 300 OR A IN (200, 300, 400, 500, 600)
   *
   * We simply remove OR and return a FilterQueryTree with leaf operator
   * -- if the number of values is 1, use EQUALITY
   * -- else use IN
   *
   * If this scenario is not applicable, we only need to look if EQUALITY/IN set
   * of child operators need to be rewritten. This is already tracked in the
   * boolean variable so we just need to check that.
   *
   * If the  variable is false, then we are done and return.
   * Else, we go over each predicate in map, rewrite it and in the end append the
   * list of non EQUALITY/IN operators (note hat these were already optimized as
   * part of collect method). Finally, we return the root of this optimized tree
   *
   * Examples:
   *
   * a IN (1) OR b IN (2) -- two IN operators and zero other operators
   * both IN operators can be rewritten to EQUALITY
   *
   * a IN (100) OR b = 300 -- one IN and one EQUALITY operator,
   * zero other operators, IN can be rewritten
   *
   * a = 100 OR b = 300 -- two equality and zero other operators
   * nothing needs to be rewritten
   *
   * a = 1 OR b = 2 OR c > 20 -- two equality and one other operator
   * nothing needs to be rewritten

   * a IN (1) OR b IN (2) OR c > 20 -- two IN operators and one other operators
   * both IN operators can be rewritten to EQUALITY and other operator need not
   * be rewritten
   *
   * a IN (1) OR b IN (2) OR (c > 20 AND a > 200)
   * two IN operators and AND other operator. both IN can be rewritten
   * and for AND we can't decide unless we drill down so we recurse.
   *
   * a = 1 OR b = 2 OR (c > 20 AND a > 200)
   * two EQUALITY operators and AND other operator.
   * both EQUALITY need not be rewritten
   * and for AND we can't decide unless we drill down so we recurse
   *
   * a = 1 OR b = 2 OR (c IN (20) AND a > 200)
   * two EQUALITY operators and AND other operator.
   * both EQUALITY need not be rewritten
   * and for AND we can't decide unless we drill down so we recurse --
   * the recursion will return the AND tree with the inner IN rewritten
   *
   * @param filterQueryTree root of tree to optimize
   * @return root of optimized tree
   */
  private FilterQueryTree optimizeTreeRootedAtOR(FilterQueryTree filterQueryTree) {
    // TODO: evaluate if we can use TreeMap and TreeSet here
    Map<String, Set<String>> columnToInEqPredicateValues = new HashMap<>();
    List<FilterQueryTree> nonEqInOperators = new ArrayList<>();

    // Collect all equality/in operators and non-equality operators
    // these are the immediate children of root OR operator
    boolean rewriteINEQChildrenOfORRootOperator = collectChildOperatorsOfRootOROperator(filterQueryTree, columnToInEqPredicateValues, nonEqInOperators);

    if (columnToInEqPredicateValues.size() == 1 && nonEqInOperators.isEmpty()) {
      // We can eliminate the OR root node if there is exactly one unique column with one or more
      // predicate values(could be duplicate) using IN/OR. Rewritten as follows:
      // -- if there is one predicate (after deduping), use EQUALITY
      // -- else use IN
      // e.g:
      // a = 1 OR a = 1 --> can be rewritten as a = 1
      // a = 1 OR a = 2 OR a = 3 --> can be rewritten as a IN (1, 2, 3)
      Map.Entry<String, Set<String>> columnAndValues = columnToInEqPredicateValues.entrySet().iterator().next();
      return buildFilterQueryTreeForColumnAndPredicateInfo(columnAndValues.getKey(), columnAndValues.getValue());
    }

    if (!rewriteINEQChildrenOfORRootOperator) {
      // No predicate rewriting, so just return the same tree
      return filterQueryTree;
    }

    // Rewrite the IN/EQUALITY child operators
    FilterQueryTree optimizedRoot = rebuildFilterPredicate(columnToInEqPredicateValues);
    // append all non EQ/IN child operators
    optimizedRoot.getChildren().addAll(nonEqInOperators);

    return optimizedRoot;
  }

  /**
   * Optimize FilterQueryTree rooted at AND. We recurse for each
   * subtree by calling optimize
   * The root (AND) is kept same and optimize is invoked on
   * each subtree and updated in the children list of root
   * @param filterQueryTree root of tree to optimize
   * @return root of optimized tree
   *
   * Examples:
   *
   * a IN (1) AND b = 3 -- rewritten to a = 1 AND b = 3
   * a IN (1) AND b IN (3) -- rewritten to a = 1 AND b = 3
   * (a = 1 OR a = 2) AND b = 3 -- rewritten to (a IN (1, 2)) AND b = 3
   * a > 20 AND a < 100 -- not rewritten
   * a = 100 AND b = 300 -- not rewritten
   * (a IN (1, 2, 3) OR b IN (4, 5, 6)) AND (a < 20 OR b > 2) -- not rewritten
   */
  private FilterQueryTree optimizeTreeRootedAtAND(FilterQueryTree filterQueryTree) {
    List<FilterQueryTree> children = filterQueryTree.getChildren();
    int numChildren = children.size();
    for (int i = 0; i < numChildren; i++) {
      FilterQueryTree childQueryTree = children.get(i);
      children.set(i, optimize(childQueryTree));
    }
    return filterQueryTree;
  }

  /**
   * Collect the immediate children of FilterQueryTree rooted at OR.
   * The children are collected in two separate data structures.
   *
   * (1) If the child operator is IN/EQUALITY, then it is stored
   * in a map of column name to a predicate info containing the unique
   * set of 1 or more predicate values and whether or not this predicate has to
   * be rewritten. We decide this on the basis of:
   *
   * If there are duplicate values or if the column has occurred multiple times
   * or if it is a single value IN predicate.
   *
   * (2) For all the other kinds of child operators (AND, <, >, BETWEEN etc),
   * we simply collect the child FilterQueryTree in a list after calling
   * optimize() on them if need be
   *
   * @param filterQueryTree root of OR tree
   * @param columnToInEqPredicateValues Map data structure to collect all
   *                                    immediate IN/EQUALITY child operators
   * @param nonEqInOperators list to collect all other immediate child
   *                         operators
   * @return true if EQ/IN operators should be rewritten, false otherwise
   */
  private boolean collectChildOperatorsOfRootOROperator(FilterQueryTree filterQueryTree,
      Map<String, Set<String>> columnToInEqPredicateValues, List<FilterQueryTree> nonEqInOperators) {
    // create the array of overall size; it doesn't matter since we
    // won't be using this array if non EQ/IN operator list is empty
    boolean rewriteINEQChildrenOfORRootOperator = false;
    for (FilterQueryTree childQueryTree : filterQueryTree.getChildren()) {
      FilterOperator operator = childQueryTree.getOperator();
      if (operator == FilterOperator.EQUALITY || operator == FilterOperator.IN) {
        // the immediate child of root OR operator is EQUALITY or IN
        List<String> childValues = childQueryTree.getValue();
        String column = childQueryTree.getColumn();
        Set<String> predicateValues = columnToInEqPredicateValues.get(column);
        if (predicateValues == null) {
          // we didn't see this column before
          predicateValues = new HashSet<>(childValues);
          // if after adding to the set, the number of predicate values become different (less),
          // it implies that values got deduplicated and so we need to rewrite this predicate later
          // similarly, if this is a single value IN predicate, we need to rewrite it later to equality
          int numChildren = childValues.size();
          if (operator == FilterOperator.IN && (numChildren == 1 || numChildren != predicateValues.size())) {
            rewriteINEQChildrenOfORRootOperator = true;
          }
          columnToInEqPredicateValues.put(column, predicateValues);
        } else {
          // if we had earlier seen this column, we need to rewrite the predicate
          predicateValues.addAll(childValues);
          rewriteINEQChildrenOfORRootOperator = true;
        }
      } else {
        // the immediate child of root OR operator is AND, RANGE, BETWEEN etc
        // only AND needs to be considered for optimization
        // rest are not candidates
        if (childQueryTree.getOperator() == FilterOperator.AND) {
          nonEqInOperators.add(optimizeTreeRootedAtAND(childQueryTree));
        } else {
          nonEqInOperators.add(childQueryTree);
        }
      }
    }
    return rewriteINEQChildrenOfORRootOperator;
  }

  private FilterQueryTree rebuildFilterPredicate(Map<String, Set<String>> columnToInEqPredicateValues) {
    ArrayList<FilterQueryTree> newChildren = new ArrayList<>();
    for (Map.Entry<String, Set<String>> columnAndPredicate : columnToInEqPredicateValues.entrySet()) {
      newChildren.add(buildFilterQueryTreeForColumnAndPredicateInfo(columnAndPredicate.getKey(), columnAndPredicate.getValue()));
    }
    return new FilterQueryTree(null, null, FilterOperator.OR, newChildren);
  }

  private FilterQueryTree buildFilterQueryTreeForColumnAndPredicateInfo(String column, Set<String> predicateValues) {
    // If there's only one value, turn it into an equality, otherwise turn it into an IN clause
    List<String> values = new ArrayList<>(predicateValues);
    if (predicateValues.size() == 1) {
      return new FilterQueryTree(column, values, FilterOperator.EQUALITY, null);
    } else {
      return new FilterQueryTree(column, values, FilterOperator.IN, null);
    }
  }
}
