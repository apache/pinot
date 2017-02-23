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
package com.linkedin.pinot.broker.requesthandler;

import com.linkedin.pinot.common.request.FilterOperator;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;


/**
 * FilterOptimizer to merge intersecting range predicates:
 * <ul>
 *   <li> Given a filter query tree with range predicates on the same column, this optimizer merges the
 *   predicates joined by AND by taking their intersection. </li>
 *   <li> Pulls up merged predicates in the absence of other predicates on other columns. </li>
 *   <li> Currently implemented to work for time column only. This is because the broker currently
 *   does not know the data type for any columns, except for time column. </li>
 * </ul>
 */
public class RangeMergeOptimizer extends FilterQueryTreeOptimizer {
  private static final String DUMMY_STRING = "__dummy_string__";

  @Override
  public FilterQueryTree optimize(FilterQueryOptimizerRequest request) {
    FilterQueryTree filterQueryTree = request.getFilterQueryTree();
    optimizeRanges(filterQueryTree, request.getTimeColumn());

    List<FilterQueryTree> children = filterQueryTree.getChildren();
    if (children != null && children.size() == 1) {
      return children.get(0);
    }
    return filterQueryTree;
  }

  /**
   * Recursive method that performs the actual optimization of merging range predicates.
   *
   * @param current Current node being visited in the DFS of the filter query tree.
   * @param timeColumn Name of time column
   * @return Returns the optimized filter query tree
   */
  private static FilterQueryTree optimizeRanges(FilterQueryTree current, String timeColumn) {
    if (current == null || timeColumn == null) {
      return current;
    }

    List<FilterQueryTree> children = current.getChildren();
    if (children == null || children.isEmpty()) {
      return current;
    }

    ListIterator<FilterQueryTree> iterator = children.listIterator();
    while (iterator.hasNext()) {
      FilterQueryTree child = iterator.next();
      optimizeRanges(child, timeColumn);

      List<FilterQueryTree> grandChildren = child.getChildren();
      if (grandChildren != null && grandChildren.size() == 1) {
        iterator.remove();
        iterator.add(grandChildren.get(0));
      }
    }

    List<String> intersect = null;
    iterator = children.listIterator();

    while (iterator.hasNext()) {
      FilterQueryTree child = iterator.next();
      FilterOperator filterOperator = child.getOperator();
      String column = child.getColumn();

      if (filterOperator != null && column != null) {
        if (column.equals(timeColumn) && filterOperator.equals(FilterOperator.RANGE)) {
          intersect = (intersect == null) ? child.getValue() : intersectRanges(intersect, child.getValue());
          iterator.remove();
        }
      }
    }

    if (intersect != null) {
      children.add(new FilterQueryTree(timeColumn, intersect, FilterOperator.RANGE, null));
    }
    return current;
  }

  /**
   * Helper method to compute intersection of two ranges.
   * Assumes that values are 'long'. This is OK as this feature is used only for time-column.
   *
   * @param range1 First range
   * @param range2 Second range
   * @return Intersection of the given ranges.
   */
  private static List<String> intersectRanges(List<String> range1, List<String> range2) {

    // Build temporary range predicates to parse the string range values.
    RangePredicate predicate1 = new RangePredicate(DUMMY_STRING, range1);
    RangePredicate predicate2 = new RangePredicate(DUMMY_STRING, range2);

    String lowerString1 = predicate1.getLowerBoundary();
    String upperString1 = predicate1.getUpperBoundary();

    long lower1 = (lowerString1.equals("*")) ? Long.MIN_VALUE : Long.valueOf(lowerString1);
    long upper1 = (upperString1.equals("*")) ? Long.MAX_VALUE : Long.valueOf(upperString1);

    String lowerString2 = predicate2.getLowerBoundary();
    String upperString2 = predicate2.getUpperBoundary();

    long lower2 = (lowerString2.equals("*")) ? Long.MIN_VALUE : Long.valueOf(lowerString2);
    long upper2 = (upperString2.equals("*")) ? Long.MAX_VALUE : Long.valueOf(upperString2);

    final StringBuilder stringBuilder = new StringBuilder();
    if (lower1 > lower2) {
      stringBuilder.append(
          (predicate1.includeLowerBoundary() ? RangePredicate.LOWER_INCLUSIVE : RangePredicate.LOWER_EXCLUSIVE));
      stringBuilder.append(lower1);
    } else if (lower1 < lower2) {
      stringBuilder.append(
          (predicate2.includeLowerBoundary() ? RangePredicate.LOWER_INCLUSIVE : RangePredicate.LOWER_EXCLUSIVE));
      stringBuilder.append(lower2);
    } else {
      if (lower1 == Long.MIN_VALUE) { // lower1 == lower2
        stringBuilder.append(RangePredicate.LOWER_EXCLUSIVE + RangePredicate.UNBOUNDED); // * always has '('
      } else {
        stringBuilder.append(
            (predicate1.includeLowerBoundary() && predicate2.includeLowerBoundary()) ? RangePredicate.LOWER_INCLUSIVE
                : RangePredicate.LOWER_EXCLUSIVE);
        stringBuilder.append(lower1);
      }
    }

    stringBuilder.append(RangePredicate.DELIMITER);
    if (upper1 < upper2) {
      stringBuilder.append(upper1);
      stringBuilder.append(
          (predicate1.includeUpperBoundary() ? RangePredicate.UPPER_INCLUSIVE : RangePredicate.UPPER_EXCLUSIVE));
    } else if (upper2 < upper1) {
      stringBuilder.append(upper2);
      stringBuilder.append(
          (predicate2.includeUpperBoundary() ? RangePredicate.UPPER_INCLUSIVE : RangePredicate.UPPER_EXCLUSIVE));
    } else {
      if (upper1 == Long.MAX_VALUE) { // lower1 == lower2
        stringBuilder.append(RangePredicate.UNBOUNDED + RangePredicate.UPPER_EXCLUSIVE); // * always has ')'
      } else {
        stringBuilder.append(lower1);
        stringBuilder.append(
            (predicate1.includeUpperBoundary() && predicate2.includeUpperBoundary()) ? RangePredicate.UPPER_INCLUSIVE
                : RangePredicate.UPPER_EXCLUSIVE);
      }
    }
    return Collections.singletonList(stringBuilder.toString());
  }
}
