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
package org.apache.pinot.broker.requesthandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.FilterQueryMap;


/**
 * The optimizer looks at the FilterQuery to convert
 *
 * (1) multiple OR equality predicates into a single IN predicte
 * e.g COL = 1 OR COl = 2 OR COL = 3 is rewritten as COL IN (1, 2, 3)
 *
 * (2) single value IN predicate into equality predicate
 * e.g COL IN (1) is rewritten as COL = 1
 *
 * (3) Deduplicates the predicate values for IN.
 */
public class MultipleOrEqualitiesToInClauseFilterQueryOptimizer extends FilterQueryOptimizer {

  @Override
  public FilterQueryOptimizationResult optimize(FilterQuery filterQuery, FilterQueryMap filterQueryMap) {
    if (filterQuery.getOperator() == FilterOperator.OR) {
      // Root operator is OR
      return optimizeTreeRootedAtOR(filterQuery, filterQueryMap);
    } else if (filterQuery.getOperator() == FilterOperator.AND) {
      // Root operator is AND
      return optimizeTreeRootedAtAND(filterQuery, filterQueryMap);
    } else {
      // if we are here, it means the query has a single predicate
      // (just one column with one or more predicate values)
      // a = 1
      // a IN (1)
      // a > 7
      // a IN (1, 2, 3, 4)
      return singlePredicateOptimization(filterQuery, filterQueryMap);
    }
  }

  private FilterQueryOptimizationResult singlePredicateOptimization(FilterQuery filterQuery,
      FilterQueryMap filterQueryMap) {
    // rewrite single predicate IN as EQUALITY, for remaining cases this optimizer is a NO-OP
    if (filterQuery.getOperator() == FilterOperator.IN && filterQuery.getValue().size() == 1) {
      filterQuery.setOperator(FilterOperator.EQUALITY);
    }
    return new FilterQueryOptimizationResult(filterQuery, filterQueryMap);
  }

  private static class PredicateInfo {
    Set<String> _predicateValues;
    boolean _multipleOccurences;
    boolean _inPredicate;

    PredicateInfo(Set<String> predicateValues, boolean multipleOccurences, boolean inPredicate) {
      _predicateValues = predicateValues;
      _multipleOccurences = multipleOccurences;
      _inPredicate = inPredicate;
    }
  }

  private FilterQueryOptimizationResult optimizeTreeRootedAtOR(FilterQuery filterQuery, FilterQueryMap filterQueryMap) {
    Map<String, PredicateInfo> columnToPredicateInfo = new HashMap<>();
    List<FilterQuery> nonEqInOperators = new ArrayList<>();

    // Collect all EQ/IN  and non EQ/IN operators separately
    boolean containsDuplicates =
        collectChildOperatorsForRoot(filterQuery, filterQueryMap, columnToPredicateInfo, nonEqInOperators);

    if (columnToPredicateInfo.size() == 1) {
      if (nonEqInOperators.isEmpty()) {
        // We can eliminate the OR root node if there is exactly one unique column with one or more
        // predicate values(could be duplicate) using IN/OR. Rewritten as follows:
        // -- if there is one predicate (after deduping), use EQUALITY
        // -- else use IN
        // e.g:
        // a = 1 OR a = 1 --> can be rewritten as a = 1
        // a = 1 OR a = 2 OR a = 3 --> can be rewritten as a IN (1, 2, 3)
        Map.Entry<String, PredicateInfo> columnAndValues = columnToPredicateInfo.entrySet().iterator().next();
        return buildFilterQueryForSingleColumn(columnAndValues.getKey(), columnAndValues.getValue()._predicateValues);
      }
    }

    // Two scenarios can lead us here:

    // SCENARIO 1: There is 1 unique column in the filter but it has other operators (<, >) and not just EQ/IN
    // e.g:
    // a IN (1) OR a > 7 -> can be rewritten as a = 1 OR a > 7
    // a = 1 OR a = 1 OR a > 7 -> can be rewritten as a = 1 OR a > 7
    // a = 1 OR a = 2 OR a > 7 -> can be rewritten as a IN (1, 2) OR a > 7
    // a = 1 OR a = 2 OR (a > 7 AND a < 20) -> can be rewritten as a IN (1, 2) OR (a > 7 AND a < 20)

    // SCENARIO 2: There are 2 or more columns in the filter
    // e.g:
    // a IN (1) OR b = 3 --> can be rewritten as a = 1 OR b = 3
    // a = 1 OR a = 2 OR b = 3 --> can be rewritten as a IN (1, 2) OR b = 3
    // a = 1 OR a = 2 OR a IN (4, 5, 6, 7) OR b = 3 --> can be rewritten as a IN (1, 2, 4, 5, 6, 7) OR b = 3
    // a = 1 OR a = 1 OR b = 3 --> can be rewritten as a = 1 OR b = 3
    // a = 1 OR a = 1 OR b = 3 OR c > 7 --> can be rewritten as a = 1 OR b = 3 OR c > 7

    boolean rewriteRequired = false;

    if (containsDuplicates) {
      rewriteRequired = true;
    } else {
      for (PredicateInfo predicateInfo : columnToPredicateInfo.values()) {
        if (predicateInfo._multipleOccurences) {
          rewriteRequired = true;
          break;
        } else if (predicateInfo._inPredicate && predicateInfo._predicateValues.size() == 1) {
          rewriteRequired = true;
          break;
        }
      }
    }

    if (!rewriteRequired) {
      // No predicate rewriting, so just return the same tree
      return new FilterQueryOptimizationResult(filterQuery, filterQueryMap);
    } else {
      // Rewrite the predicates
      return rewriteFilterPredicatesWithORRootOperator(columnToPredicateInfo, nonEqInOperators, filterQueryMap);
    }
  }

  private boolean collectChildOperatorsForRoot(FilterQuery filterQuery, FilterQueryMap filterQueryMap,
      Map<String, PredicateInfo> columnToPredicateInfo, List<FilterQuery> nonEqInOperators) {
    boolean containsDuplicates = false;
    Map<Integer, FilterQuery> filterSubQueryMap = filterQueryMap.getFilterQueryMap();
    for (int childFilterQueryId : filterQuery.getNestedFilterQueryIds()) {
      FilterQuery childFilterQuery = filterSubQueryMap.get(childFilterQueryId);
      FilterOperator operator = childFilterQuery.getOperator();
      if (operator == FilterOperator.EQUALITY || operator == FilterOperator.IN) {
        List<String> childValues = childFilterQuery.getValue();
        String column = childFilterQuery.getColumn();
        PredicateInfo predicateInfo;
        if (!columnToPredicateInfo.containsKey(column)) {
          predicateInfo = new PredicateInfo(new HashSet<>(), false, operator == FilterOperator.IN);
          columnToPredicateInfo.put(column, predicateInfo);
        } else {
          predicateInfo = columnToPredicateInfo.get(column);
          predicateInfo._multipleOccurences = true;
        }
        for (String childValue : childValues) {
          if (!predicateInfo._predicateValues.add(childValue)) {
            containsDuplicates = true;
          }
        }
      } else {
        // <, >, AND ....
        nonEqInOperators.add(childFilterQuery);
      }
    }

    return containsDuplicates;
  }

  // e.g
  // a IN (1) AND b = 3 -> can be rewritten as a = 1 AND b = 3
  // (a = 1 OR a = 2) AND b = 3 -> can be rewritten as a IN (1, 2) AND b = 3
  // (a = 1 OR a = 1) AND b = 3 -> can be rewritten as
  // (a = 1 OR a = 2) AND b = 3 AND c > 7
  // (a = 1 OR a IN (1, 2, 3) OR b IN (3)) AND (b < 20 OR c > 20) -> (a IN (1, 2, 3) OR b = 3) AND (b < 20 OR c > 20)
  private FilterQueryOptimizationResult optimizeTreeRootedAtAND(FilterQuery filterQuery,
      FilterQueryMap filterQueryMap) {
    Map<Integer, FilterQuery> filterSubQueryMap = filterQueryMap.getFilterQueryMap();
    List<FilterQueryOptimizationResult> optimizationResults = new ArrayList<>();
    boolean optimized = false;

    for (int childFilterQueryId : filterQuery.getNestedFilterQueryIds()) {
      FilterQuery childFilterQueryToOptimize = filterSubQueryMap.get(childFilterQueryId);
      FilterQueryOptimizationResult filterQueryOptimizationResult =
          optimize(childFilterQueryToOptimize, filterQueryMap);
      optimizationResults.add(filterQueryOptimizationResult);
      if (!optimized && filterQueryOptimizationResult._filterQuery != childFilterQueryToOptimize) {
        // if the returned FilterQuery is not the same as provided by BrokerRequest then
        // we know it is not rewritten/optimized
        optimized = true;
      }
    }

    if (optimized) {
      FilterQuery root = new FilterQuery();
      FilterQueryMap updatedFilterQueryMap = new FilterQueryMap();
      Map<Integer, FilterQuery> updatedFilterSubQueryMap = new HashMap<>();
      updatedFilterQueryMap.setFilterQueryMap(updatedFilterSubQueryMap);

      // root operator should be AND
      root.setColumn(null);
      root.setValue(null);
      root.setOperator(FilterOperator.AND);
      root.setId(0);

      updatedFilterSubQueryMap.put(0, root);

      MutableInt id = new MutableInt(0);
      List<Integer> childQueryIdsForRoot = new ArrayList<>();

      for (FilterQueryOptimizationResult optimizationResult : optimizationResults) {
        id.increment();
        childQueryIdsForRoot.add(id.intValue());
        traverseFilterQueryRecursivelyToUpdateIds(id, optimizationResult._filterQuery,
            optimizationResult._filterQueryMap.getFilterQueryMap(), updatedFilterSubQueryMap);
      }

      root.setNestedFilterQueryIds(childQueryIdsForRoot);

      return new FilterQueryOptimizationResult(root, updatedFilterQueryMap);
    } else {
      return new FilterQueryOptimizationResult(filterQuery, filterQueryMap);
    }
  }

  private void traverseFilterQueryRecursivelyToUpdateIds(
      MutableInt id, FilterQuery filterQuery,
      Map<Integer, FilterQuery> filterSubQueryMap,
      Map<Integer, FilterQuery> updatedFilterSubQueryMap) {
    filterQuery.setId(id.intValue());
    updatedFilterSubQueryMap.put(id.intValue(), filterQuery);
    List<Integer> nestedQueryIds = filterQuery.getNestedFilterQueryIds();
    for (int i = 0; i < nestedQueryIds.size(); i++) {
      int nestedQueryId = nestedQueryIds.get(i);
      id.increment();
      nestedQueryIds.set(i, id.intValue());
      FilterQuery childFilterQuery = filterSubQueryMap.get(nestedQueryId);
      traverseFilterQueryRecursivelyToUpdateIds(id, childFilterQuery, filterSubQueryMap, updatedFilterSubQueryMap);
    }
  }

  // e.g:
  // a = 1 OR a = 2 OR b = 3
  // a = 1 OR a = 1 OR b = 3
  // a = 1 OR a = 1 OR b = 3 OR c > 7
  // a = 1 OR a = 1 OR (b = 3 AND c > 7)
  private FilterQueryOptimizationResult rewriteFilterPredicatesWithORRootOperator(
      Map<String, PredicateInfo> columnToPredicateInfo,
      List<FilterQuery> nonEqInOperators,
      FilterQueryMap filterQueryMap) {
    List<Integer> childQueryIdsForRoot = new ArrayList<>();
    FilterQueryMap updatedFilterQueryMap = new FilterQueryMap();
    Map<Integer, FilterQuery> updatedFilterSubQueryMap = new HashMap<>();
    updatedFilterQueryMap.setFilterQueryMap(updatedFilterSubQueryMap);

    // root operator should be OR
    FilterQuery root = new FilterQuery();
    root.setColumn(null);
    root.setValue(null);
    root.setOperator(FilterOperator.OR);
    root.setId(0);
    root.setNestedFilterQueryIds(childQueryIdsForRoot);

    updatedFilterSubQueryMap.put(0, root);

    MutableInt id = new MutableInt(0);

    for (Map.Entry<String, PredicateInfo> columnAndPredicateInfo : columnToPredicateInfo.entrySet()) {
      id.increment();
      String column = columnAndPredicateInfo.getKey();
      Set<String> predicateValues = columnAndPredicateInfo.getValue()._predicateValues;
      FilterQuery fq = buildFilterQueryWithINorOR(column, predicateValues, id.intValue());
      childQueryIdsForRoot.add(fq.getId());
      updatedFilterSubQueryMap.put(fq.getId(), fq);
    }

    for (FilterQuery nonEqInFilterQuery : nonEqInOperators) {
      FilterQueryOptimizationResult optimizationResult = optimize(nonEqInFilterQuery, filterQueryMap);
      id.increment();
      childQueryIdsForRoot.add(id.intValue());
      traverseNonEQINOperators(id, optimizationResult._filterQuery,
          optimizationResult._filterQueryMap.getFilterQueryMap(), updatedFilterSubQueryMap);
    }

    return new FilterQueryOptimizationResult(root, updatedFilterQueryMap);
  }

  private void traverseNonEQINOperators(
      MutableInt id,
      FilterQuery filterQuery,
      Map<Integer, FilterQuery> filterSubQueryMap,
      Map<Integer, FilterQuery> updatedFilterSubQueryMap) {
    filterQuery.setId(id.intValue());
    updatedFilterSubQueryMap.put(id.intValue(), filterQuery);
    List<Integer> nestedQueryIds = filterQuery.getNestedFilterQueryIds();
    for (int i = 0; i < nestedQueryIds.size(); i++) {
      id.increment();
      int origId = nestedQueryIds.get(i);
      nestedQueryIds.set(i, id.intValue());
      traverseNonEQINOperators(id, filterSubQueryMap.get(origId), filterSubQueryMap, updatedFilterSubQueryMap);
    }
  }

  private FilterQueryOptimizationResult buildFilterQueryForSingleColumn(String column, Set<String> predicateValues) {
    FilterQuery root = buildFilterQueryWithINorOR(column, predicateValues, 0);
    FilterQueryMap filterQueryMap = new FilterQueryMap();
    Map<Integer, FilterQuery> map = new HashMap<>();
    map.put(0, root);
    filterQueryMap.setFilterQueryMap(map);
    return new FilterQueryOptimizationResult(root, filterQueryMap);
  }

  private FilterQuery buildFilterQueryWithINorOR(String column, Set<String> predicateValues, int id) {
    FilterQuery filterQuery = new FilterQuery();
    filterQuery.setColumn(column);
    filterQuery.setId(id);
    filterQuery.setNestedFilterQueryIds(Collections.emptyList());
    filterQuery.setValue(new ArrayList<>(predicateValues));

    // If there's only one value, turn it into an equality, otherwise turn it into an IN clause
    if (predicateValues.size() == 1) {
      filterQuery.setOperator(FilterOperator.EQUALITY);
    } else {
      filterQuery.setOperator(FilterOperator.IN);
    }

    return filterQuery;
  }
}