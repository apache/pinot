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
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.predicate.RangePredicate;


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
  private static final ExpressionContext DUMMY_EXPRESSION = ExpressionContext.forIdentifier("dummy");

  @Override
  public FilterQueryTree optimize(FilterQueryOptimizerRequest request) {
    return optimizeRanges(request.getFilterQueryTree(), request.getTimeColumn());
  }

  /**
   * Recursive method that performs the actual optimization of merging range predicates.
   *
   * @param current Current node being visited in the DFS of the filter query tree.
   * @param timeColumn Name of time column
   * @return Returns the optimized filter query tree
   */
  private static FilterQueryTree optimizeRanges(FilterQueryTree current, @Nullable String timeColumn) {
    if (timeColumn == null) {
      return current;
    }

    List<FilterQueryTree> children = current.getChildren();
    if (children == null || children.isEmpty()) {
      return current;
    }

    // For OR, we optimize all its children, but do not propagate up.
    FilterOperator operator = current.getOperator();
    if (operator == FilterOperator.OR) {
      int length = children.size();
      for (int i = 0; i < length; i++) {
        children.set(i, optimizeRanges(children.get(i), timeColumn));
      }
      return current;
    }

    // After this point, since the node has children, it can only be an 'AND' node (only OR/AND supported).
    assert operator == FilterOperator.AND;
    List<FilterQueryTree> newChildren = new ArrayList<>();
    String mergedRange = null;
    for (FilterQueryTree child : children) {
      FilterQueryTree newChild = optimizeRanges(child, timeColumn);
      if (newChild.getOperator() == FilterOperator.RANGE && newChild.getColumn().equals(timeColumn)) {
        String range = newChild.getValue().get(0);
        if (mergedRange == null) {
          mergedRange = range;
        } else {
          mergedRange = intersectRanges(mergedRange, range);
        }
      } else {
        newChildren.add(newChild);
      }
    }

    FilterQueryTree rangeFilter =
        new FilterQueryTree(timeColumn, Collections.singletonList(mergedRange), FilterOperator.RANGE, null);
    if (newChildren.isEmpty()) {
      return rangeFilter;
    } else {
      if (mergedRange != null) {
        newChildren.add(rangeFilter);
      }
      return new FilterQueryTree(null, null, FilterOperator.AND, newChildren);
    }
  }

  /**
   * Helper method to compute intersection of two ranges.
   * Assumes that values are 'long'. This is OK as this feature is used only for time-column.
   *
   * @param range1 First range
   * @param range2 Second range
   * @return Intersection of the given ranges.
   */
  public static String intersectRanges(String range1, String range2) {
    // Build temporary range predicates to parse the string range values.
    RangePredicate predicate1 = new RangePredicate(DUMMY_EXPRESSION, range1);
    RangePredicate predicate2 = new RangePredicate(DUMMY_EXPRESSION, range2);
    StringBuilder stringBuilder = new StringBuilder();

    String lowerBound1 = predicate1.getLowerBound();
    String lowerBound2 = predicate2.getLowerBound();
    if (lowerBound1.equals(RangePredicate.UNBOUNDED)) {
      stringBuilder
          .append(predicate2.isLowerInclusive() ? RangePredicate.LOWER_INCLUSIVE : RangePredicate.LOWER_EXCLUSIVE)
          .append(lowerBound2);
    } else if (lowerBound2.equals(RangePredicate.UNBOUNDED)) {
      stringBuilder
          .append(predicate1.isLowerInclusive() ? RangePredicate.LOWER_INCLUSIVE : RangePredicate.LOWER_EXCLUSIVE)
          .append(lowerBound1);
    } else {
      long lowerValue1 = Long.parseLong(lowerBound1);
      long lowerValue2 = Long.parseLong(lowerBound2);
      if (lowerValue1 < lowerValue2) {
        stringBuilder
            .append(predicate2.isLowerInclusive() ? RangePredicate.LOWER_INCLUSIVE : RangePredicate.LOWER_EXCLUSIVE)
            .append(lowerBound2);
      } else if (lowerValue1 > lowerValue2) {
        stringBuilder
            .append(predicate1.isLowerInclusive() ? RangePredicate.LOWER_INCLUSIVE : RangePredicate.LOWER_EXCLUSIVE)
            .append(lowerBound1);
      } else {
        stringBuilder.append(
            predicate1.isLowerInclusive() && predicate2.isLowerInclusive() ? RangePredicate.LOWER_INCLUSIVE
                : RangePredicate.LOWER_EXCLUSIVE).append(lowerBound1);
      }
    }

    stringBuilder.append(RangePredicate.DELIMITER);

    String upperBound1 = predicate1.getUpperBound();
    String upperBound2 = predicate2.getUpperBound();
    if (upperBound1.equals(RangePredicate.UNBOUNDED)) {
      stringBuilder.append(upperBound2)
          .append(predicate2.isUpperInclusive() ? RangePredicate.UPPER_INCLUSIVE : RangePredicate.UPPER_EXCLUSIVE);
    } else if (upperBound2.equals(RangePredicate.UNBOUNDED)) {
      stringBuilder.append(upperBound1)
          .append(predicate1.isUpperInclusive() ? RangePredicate.UPPER_INCLUSIVE : RangePredicate.UPPER_EXCLUSIVE);
    } else {
      long upperValue1 = Long.parseLong(upperBound1);
      long upperValue2 = Long.parseLong(upperBound2);
      if (upperValue1 < upperValue2) {
        stringBuilder.append(upperBound1)
            .append(predicate1.isUpperInclusive() ? RangePredicate.UPPER_INCLUSIVE : RangePredicate.UPPER_EXCLUSIVE);
      } else if (upperValue1 > upperValue2) {
        stringBuilder.append(upperBound2)
            .append(predicate2.isUpperInclusive() ? RangePredicate.UPPER_INCLUSIVE : RangePredicate.UPPER_EXCLUSIVE);
      } else {
        stringBuilder.append(upperBound1).append(
            predicate1.isUpperInclusive() && predicate2.isUpperInclusive() ? RangePredicate.UPPER_INCLUSIVE
                : RangePredicate.UPPER_EXCLUSIVE);
      }
    }

    return stringBuilder.toString();
  }
}
