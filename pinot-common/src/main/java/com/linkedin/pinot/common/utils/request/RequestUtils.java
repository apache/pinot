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
package com.linkedin.pinot.common.utils.request;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.FilterQueryMap;


public class RequestUtils {

  /**
   * Generates thrift compliant filterQuery and populate it in the broker request
   * @param filterQueryTree
   * @param request
   */
  public static void generateFilterFromTree(FilterQueryTree filterQueryTree, BrokerRequest request) {
    Map<Integer, FilterQuery> filterQueryMap = new HashMap<Integer, FilterQuery>();
    FilterQuery root = traverseFilterQueryAndPopulateMap(filterQueryTree, filterQueryMap);
    filterQueryMap.put(root.getId(), root);
    request.setFilterQuery(root);
    FilterQueryMap mp = new FilterQueryMap();
    mp.setFilterQueryMap(filterQueryMap);
    request.setFilterSubQueryMap(mp);
  }

  private static FilterQuery traverseFilterQueryAndPopulateMap(FilterQueryTree tree,
      Map<Integer, FilterQuery> filterQueryMap) {
    final List<Integer> f = new ArrayList<Integer>();
    if (null != tree.getChildren()) {
      for (final FilterQueryTree c : tree.getChildren()) {
        f.add(c.getId());
        final FilterQuery q = traverseFilterQueryAndPopulateMap(c, filterQueryMap);
        filterQueryMap.put(c.getId(), q);
      }
    }
    FilterQuery query = new FilterQuery();
    query.setColumn(tree.getColumn());
    query.setId(tree.getId());
    query.setNestedFilterQueryIds(f);
    query.setOperator(tree.getOperator());
    query.setValue(tree.getValue());
    return query;
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

//    flatten(root, null);

    return root;
  }

  /**
   * Flatten the operators if parent and child have the same AND or OR operator.
   * (e.g. AND( a, AND (b, c)) is the same as AND(a, b, c). This helps when we re-order
   * operators for performance.
   *
   * It does so by looking at the operator of the 'parent' and 'node'. If they are same, and
   * collapsible, then all the children of 'node' are moved one level up to be siblings of
   * 'node', rendering 'node' childless. 'node' is then removed from 'parent's children list.
   *
   * @param node The node whose children are to be moved up one level if criteria is satisfied.
   * @param parent Node's parent who will inherit node's children if criteria is satisfied.
   */
  private static void flatten(FilterQueryTree node, FilterQueryTree parent) {
    if (node == null || node.getChildren() == null) {
      return;
    }
    // Flatten all the children first.
    List<FilterQueryTree> toFlatten = new ArrayList<>(node.getChildren().size());
    for (FilterQueryTree child: node.getChildren()) {
      if (child.getChildren() != null && !child.getChildren().isEmpty()) {
        toFlatten.add(child);
      }
    }
    for (FilterQueryTree child : toFlatten) {
      flatten(child, node);
    }
    if (parent == null) {
      return;
    }
    if (node.getOperator() == parent.getOperator() &&
        (node.getOperator() == FilterOperator.OR || node.getOperator() == FilterOperator.AND)) {
      // Move all of 'node's children one level up. If 'node' has no children left, remove it from parent's list.
      List<FilterQueryTree> children = node.getChildren();
      Iterator<FilterQueryTree> it = children.iterator();
      while (it.hasNext()) {
        parent.getChildren().add(it.next());
        it.remove();
      }
      // 'node' is now childless
      // Remove this node from its parent's list.
      parent.getChildren().remove(node);
    }
  }

  private static FilterQueryTree buildFilterQuery(Integer id, Map<Integer, FilterQuery> queryMap) {
    FilterQuery q = queryMap.get(id);

    List<Integer> children = q.getNestedFilterQueryIds();

    List<FilterQueryTree> c = null;
    if (null != children && !children.isEmpty()) {
      c = new ArrayList<FilterQueryTree>();
      for (final Integer i : children) {
        final FilterQueryTree t = buildFilterQuery(i, queryMap);
        c.add(t);
      }
    }

    FilterQueryTree q2 = new FilterQueryTree(id, q.getColumn(), q.getValue(), q.getOperator(), c);
    return q2;
  }
}
