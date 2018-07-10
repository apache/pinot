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
package com.linkedin.pinot.broker.requesthandler;

import com.google.common.base.Splitter;
import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


/**
 * Optimizer that collapses multiple OR clauses to IN clauses. For example, <code>a = 1 OR a = 2 OR a =
 * 3</code> gets turned to <code>a IN (1, 2, 3)</code>.
 */
public class MultipleOrEqualitiesToInClauseFilterQueryTreeOptimizer extends FilterQueryTreeOptimizer {
  @Override
  public FilterQueryTree optimize(FilterQueryOptimizerRequest request) {
    return optimize(request.getFilterQueryTree(), null);
  }

  private FilterQueryTree optimize(FilterQueryTree filterQueryTree, FilterQueryTree parent) {
    if (filterQueryTree.getOperator() == FilterOperator.OR) {
      Map<String, Set<String>> columnToValues = new HashMap<>();
      List<FilterQueryTree> nonEqualityOperators = new ArrayList<>();

      // Collect all equality/in values and non-equality operators
      boolean containsDuplicates = collectChildOperators(filterQueryTree, columnToValues, nonEqualityOperators);

      // If we have at least one column to return
      if (!columnToValues.isEmpty()) {
        // We can eliminate the OR node if there is only one column with multiple values
        if (columnToValues.size() == 1 && nonEqualityOperators.isEmpty()) {
          Map.Entry<String, Set<String>> columnAndValues = columnToValues.entrySet().iterator().next();

          return buildFilterQueryTreeForColumnAndValues(columnAndValues);
        }

        // Check if we need to rebuild the predicate
        boolean rebuildRequired = isRebuildRequired(columnToValues, containsDuplicates);

        if (!rebuildRequired) {
          // No mutation needed, so just return the same tree
          return filterQueryTree;
        } else {
          // Rebuild the predicates
          return rebuildFilterPredicate(columnToValues, nonEqualityOperators);
        }
      }
    } else if (filterQueryTree.getChildren() != null){
      // Optimize the child nodes, if any
      applyOptimizationToChildNodes(filterQueryTree);
    }

    return filterQueryTree;
  }

  private void applyOptimizationToChildNodes(FilterQueryTree filterQueryTree) {
    Iterator<FilterQueryTree> childTreeIterator = filterQueryTree.getChildren().iterator();
    List<FilterQueryTree> childrenToAdd = null;

    while (childTreeIterator.hasNext()) {
      FilterQueryTree childQueryTree = childTreeIterator.next();
      FilterQueryTree optimizedChildQueryTree = optimize(childQueryTree, filterQueryTree);
      if (childQueryTree != optimizedChildQueryTree) {
        childTreeIterator.remove();
        if (childrenToAdd == null) {
          childrenToAdd = new ArrayList<>();
        }
        childrenToAdd.add(optimizedChildQueryTree);
      }
    }

    if (childrenToAdd != null) {
      filterQueryTree.getChildren().addAll(childrenToAdd);
    }
  }

  private boolean collectChildOperators(FilterQueryTree filterQueryTree, Map<String, Set<String>> columnToValues,
      List<FilterQueryTree> nonEqualityOperators) {
    boolean containsDuplicates = false;

    for (FilterQueryTree childQueryTree : filterQueryTree.getChildren()) {
      if (childQueryTree.getOperator() == FilterOperator.EQUALITY || childQueryTree.getOperator() == FilterOperator.IN) {
        List<String> childValues = valueDoubleTabListToElements(childQueryTree.getValue());

        if (!columnToValues.containsKey(childQueryTree.getColumn())) {
          TreeSet<String> value = new TreeSet<>(childValues);
          columnToValues.put(childQueryTree.getColumn(), value);
          if (!containsDuplicates && value.size() != childValues.size()) {
            containsDuplicates = true;
          }
        } else {
           Set<String> currentValues = columnToValues.get(childQueryTree.getColumn());
          for (String childValue : childValues) {
            if (!containsDuplicates && currentValues.contains(childValue)) {
              containsDuplicates = true;
            } else {
              currentValues.add(childValue);
            }
          }
        }
      } else {
        nonEqualityOperators.add(childQueryTree);
      }
    }

    return containsDuplicates;
  }

  private boolean isRebuildRequired(Map<String, Set<String>> columnToValues, boolean containsDuplicates) {
    // We need to rebuild the predicate if there were duplicate values detected (eg. a = 1 OR a = 1) or if there is
    // more than one value for a column (eg. a = 1 OR a = 2)
    boolean rebuildRequired = containsDuplicates;

    if (!rebuildRequired) {
      for (Set<String> columnValues : columnToValues.values()) {
        if (1 < columnValues.size()) {
          rebuildRequired = true;
          break;
        }
      }
    }
    return rebuildRequired;
  }

  private FilterQueryTree rebuildFilterPredicate(Map<String, Set<String>> columnToValues,
      List<FilterQueryTree> nonEqualityOperators) {
    ArrayList<FilterQueryTree> newChildren = new ArrayList<>();
    for (Map.Entry<String, Set<String>> columnAndValues : columnToValues.entrySet()) {
      newChildren.add(buildFilterQueryTreeForColumnAndValues(columnAndValues));
    }
    newChildren.addAll(nonEqualityOperators);
    return new FilterQueryTree(null, null, FilterOperator.OR, newChildren);
  }

  private FilterQueryTree buildFilterQueryTreeForColumnAndValues(Map.Entry<String, Set<String>> columnAndValues) {
    // If there's only one value, turn it into an equality, otherwise turn it into an IN clause
    if (columnAndValues.getValue().size() == 1) {
      return new FilterQueryTree(columnAndValues.getKey(),
          new ArrayList<>(columnAndValues.getValue()), FilterOperator.EQUALITY, null);
    } else {
      return new FilterQueryTree(columnAndValues.getKey(), new ArrayList<>(columnAndValues.getValue()),
          FilterOperator.IN, null);
    }
  }

  private List<String> valueDoubleTabListToElements(List<String> doubleTabSeparatedElements) {
    Splitter valueSplitter = Splitter.on("\t\t");
    List<String> valueElements = new ArrayList<>();

    for (String value : doubleTabSeparatedElements) {
      valueElements.addAll(valueSplitter.splitToList(value));
    }

    return valueElements;
  }
}
