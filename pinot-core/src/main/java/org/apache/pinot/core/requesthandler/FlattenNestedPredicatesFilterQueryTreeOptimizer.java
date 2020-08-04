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
import java.util.Iterator;
import java.util.List;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.utils.request.FilterQueryTree;


/**
 * Optimizer that flattens nested logical operators of the same kind. For example, AND( a, AND (b, c)) is the same as
 * AND(a, b, c).
 */
public class FlattenNestedPredicatesFilterQueryTreeOptimizer extends FilterQueryTreeOptimizer {
  public static int MAX_OPTIMIZING_DEPTH = 5;

  @Override
  public FilterQueryTree optimize(FilterQueryOptimizerRequest request) {
    FilterQueryTree filterQueryTree = request.getFilterQueryTree();
    flatten(filterQueryTree, null, MAX_OPTIMIZING_DEPTH);

    return filterQueryTree;
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
   * @param maxDepth is the maximum depth to which we recurse
   */
  private void flatten(FilterQueryTree node, FilterQueryTree parent, int maxDepth) {
    if (node == null || node.getChildren() == null || maxDepth == 0) {
      return;
    }
    // Flatten all the children first.
    List<FilterQueryTree> toFlatten = new ArrayList<>(node.getChildren().size());
    for (FilterQueryTree child : node.getChildren()) {
      if (child.getChildren() != null && !child.getChildren().isEmpty()) {
        toFlatten.add(child);
      }
    }
    for (FilterQueryTree child : toFlatten) {
      flatten(child, node, maxDepth - 1);
    }
    if (parent == null) {
      return;
    }
    if (node.getOperator() == parent.getOperator() && (node.getOperator() == FilterOperator.OR
        || node.getOperator() == FilterOperator.AND)) {
      // Move all of 'node's children one level up. If 'node' has no children left, remove it from parent's list.
      List<FilterQueryTree> children = node.getChildren();
      Iterator<FilterQueryTree> it = children.iterator();
      int insertIdx = parent.getChildren().indexOf(node);
      while (it.hasNext()) {
        parent.getChildren().add(insertIdx++, it.next());
        it.remove();
      }
      // 'node' is now childless
      // Remove this node from its parent's list.
      parent.getChildren().remove(node);
    }
  }
}
