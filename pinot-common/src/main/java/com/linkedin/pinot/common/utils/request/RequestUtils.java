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
package com.linkedin.pinot.common.utils.request;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.FilterQueryMap;
import com.linkedin.pinot.common.request.HavingFilterQuery;
import com.linkedin.pinot.common.request.HavingFilterQueryMap;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import org.apache.commons.lang.mutable.MutableInt;


public class RequestUtils {
  private RequestUtils() {
  }

  /**
   * Generates thrift compliant filterQuery and populate it in the broker request
   * @param filterQueryTree
   * @param request
   */
  public static void generateFilterFromTree(FilterQueryTree filterQueryTree, BrokerRequest request) {
    Map<Integer, FilterQuery> filterQueryMap = new HashMap<>();
    MutableInt currentId = new MutableInt(0);
    FilterQuery root = traverseFilterQueryAndPopulateMap(filterQueryTree, filterQueryMap, currentId);
    filterQueryMap.put(root.getId(), root);
    request.setFilterQuery(root);
    FilterQueryMap mp = new FilterQueryMap();
    mp.setFilterQueryMap(filterQueryMap);
    request.setFilterSubQueryMap(mp);
  }

  public static void generateFilterFromTree(HavingQueryTree filterQueryTree, BrokerRequest request) {
    Map<Integer, HavingFilterQuery> filterQueryMap = new HashMap<>();
    MutableInt currentId = new MutableInt(0);
    HavingFilterQuery root = traverseHavingFilterQueryAndPopulateMap(filterQueryTree, filterQueryMap, currentId);
    filterQueryMap.put(root.getId(), root);
    request.setHavingFilterQuery(root);
    HavingFilterQueryMap mp = new HavingFilterQueryMap();
    mp.setFilterQueryMap(filterQueryMap);
    request.setHavingFilterSubQueryMap(mp);
  }

  private static FilterQuery traverseFilterQueryAndPopulateMap(FilterQueryTree tree,
      Map<Integer, FilterQuery> filterQueryMap, MutableInt currentId) {
    int currentNodeId = currentId.intValue();
    currentId.increment();

    final List<Integer> f = new ArrayList<>();
    if (null != tree.getChildren()) {
      for (final FilterQueryTree c : tree.getChildren()) {
        int childNodeId = currentId.intValue();
        currentId.increment();

        f.add(childNodeId);
        final FilterQuery q = traverseFilterQueryAndPopulateMap(c, filterQueryMap, currentId);
        filterQueryMap.put(childNodeId, q);
      }
    }

    FilterQuery query = new FilterQuery();
    query.setColumn(tree.getColumn());
    query.setId(currentNodeId);
    query.setNestedFilterQueryIds(f);
    query.setOperator(tree.getOperator());
    query.setValue(tree.getValue());
    return query;
  }

  private static HavingFilterQuery traverseHavingFilterQueryAndPopulateMap(HavingQueryTree tree,
      Map<Integer, HavingFilterQuery> filterQueryMap, MutableInt currentId) {
    int currentNodeId = currentId.intValue();
    currentId.increment();

    final List<Integer> filterIds = new ArrayList<>();
    if (null != tree.getChildren()) {
      for (final HavingQueryTree child : tree.getChildren()) {
        int childNodeId = currentId.intValue();
        currentId.increment();
        filterIds.add(childNodeId);
        final HavingFilterQuery filterQuery = traverseHavingFilterQueryAndPopulateMap(child, filterQueryMap, currentId);
        filterQueryMap.put(childNodeId, filterQuery);
      }
    }

    HavingFilterQuery havingFilterQuery = new HavingFilterQuery();
    havingFilterQuery.setAggregationInfo(tree.getAggregationInfo());
    havingFilterQuery.setId(currentNodeId);
    havingFilterQuery.setNestedFilterQueryIds(filterIds);
    havingFilterQuery.setOperator(tree.getOperator());
    havingFilterQuery.setValue(tree.getValue());
    return havingFilterQuery;
  }

  /**
   * Generate FilterQueryTree from Broker Request
   * @param request Broker Request
   * @return
   */
  public static FilterQueryTree generateFilterQueryTree(BrokerRequest request) {
    FilterQueryTree root = null;

    FilterQuery q = request.getFilterQuery();

    if (null != q && null != request.getFilterSubQueryMap()) {
      root = buildFilterQuery(q.getId(), request.getFilterSubQueryMap().getFilterQueryMap());
    }

    return root;
  }

  public static FilterQueryTree buildFilterQuery(Integer id, Map<Integer, FilterQuery> queryMap) {
    FilterQuery q = queryMap.get(id);

    List<Integer> children = q.getNestedFilterQueryIds();

    List<FilterQueryTree> c = null;
    if (null != children && !children.isEmpty()) {
      c = new ArrayList<>();
      for (final Integer i : children) {
        final FilterQueryTree t = buildFilterQuery(i, queryMap);
        c.add(t);
      }
    }

    return new FilterQueryTree(q.getColumn(), q.getValue(), q.getOperator(), c);
  }

  /**
   * Extracts all columns from the given filter query tree.
   */
  public static Set<String> extractFilterColumns(FilterQueryTree root) {
    Set<String> filterColumns = new HashSet<>();
    if (root.getChildren() == null) {
      filterColumns.add(root.getColumn());
    } else {
      Stack<FilterQueryTree> stack = new Stack<>();
      stack.add(root);
      while (!stack.empty()) {
        FilterQueryTree node = stack.pop();
        for (FilterQueryTree child : node.getChildren()) {
          if (child.getChildren() == null) {
            filterColumns.add(child.getColumn());
          } else {
            stack.push(child);
          }
        }
      }
    }
    return filterColumns;
  }

  /**
   * Extracts all columns from the given expressions.
   */
  public static Set<String> extractColumnsFromExpressions(Set<TransformExpressionTree> expressions) {
    Set<String> expressionColumns = new HashSet<>();
    for (TransformExpressionTree expression : expressions) {
      expression.getColumns(expressionColumns);
    }
    return expressionColumns;
  }

  /**
   * Extracts all columns from the given selection, '*' will be ignored.
   */
  public static Set<String> extractSelectionColumns(Selection selection) {
    Set<String> selectionColumns = new HashSet<>();
    for (String selectionColumn : selection.getSelectionColumns()) {
      if (!selectionColumn.equals("*")) {
        selectionColumns.add(selectionColumn);
      }
    }
    if (selection.getSelectionSortSequence() != null) {
      for (SelectionSort selectionSort : selection.getSelectionSortSequence()) {
        selectionColumns.add(selectionSort.getColumn());
      }
    }
    return selectionColumns;
  }
}
