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
package org.apache.pinot.core.query.utils;

import java.util.Comparator;
import java.util.List;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.spi.exception.BadQueryRequestException;


/**
 * A utility class used to create comparators that should be used by operators that implements order by semantics.
 */
public class OrderByComparatorFactory {
  private OrderByComparatorFactory() {
  }

  public static Comparator<Object[]> getComparator(List<OrderByExpressionContext> orderByExpressions,
      ColumnContext[] orderByColumnContexts, boolean nullHandlingEnabled) {
    return getComparator(orderByExpressions, orderByColumnContexts, nullHandlingEnabled, 0, orderByExpressions.size());
  }

  public static Comparator<Object[]> getComparator(List<OrderByExpressionContext> orderByExpressions,
      ColumnContext[] orderByColumnContexts, boolean nullHandlingEnabled, int from, int to) {
    assert 0 <= from && from < to && to <= orderByExpressions.size();

    // Check if all expressions are single-valued
    for (int i = from; i < to; i++) {
      if (!orderByColumnContexts[i].isSingleValue()) {
        // MV columns should not be part of the selection order-by list
        throw new BadQueryRequestException(
            String.format("MV expression: %s should not be included in the ORDER-BY clause",
                orderByExpressions.get(i)));
      }
    }

    return getComparator(orderByExpressions, nullHandlingEnabled, from, to);
  }

  public static Comparator<Object[]> getComparator(List<OrderByExpressionContext> orderByExpressions,
      boolean nullHandlingEnabled) {
    return getComparator(orderByExpressions, nullHandlingEnabled, 0, orderByExpressions.size());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static Comparator<Object[]> getComparator(List<OrderByExpressionContext> orderByExpressions,
      boolean nullHandlingEnabled, int from, int to) {
    assert 0 <= from && from < to && to <= orderByExpressions.size();

    // Use multiplier -1 or 1 to control ascending/descending order
    int[] multipliers = new int[to];
    // Use nulls multiplier -1 or 1 to control nulls last/first order
    int[] nullsMultipliers = new int[to];
    for (int i = from; i < to; i++) {
      multipliers[i] = orderByExpressions.get(i).isAsc() ? 1 : -1;
      nullsMultipliers[i] = orderByExpressions.get(i).isNullsLast() ? 1 : -1;
    }

    if (nullHandlingEnabled) {
      return (Object[] o1, Object[] o2) -> {
        for (int i = from; i < to; i++) {
          Comparable v1 = (Comparable) o1[i];
          Comparable v2 = (Comparable) o2[i];
          if (v1 == null && v2 == null) {
            continue;
          } else if (v1 == null) {
            return nullsMultipliers[i];
          } else if (v2 == null) {
            return -nullsMultipliers[i];
          }
          int result = v1.compareTo(v2);
          if (result != 0) {
            return result * multipliers[i];
          }
        }
        return 0;
      };
    } else {
      return (Object[] o1, Object[] o2) -> {
        for (int i = from; i < to; i++) {
          Comparable v1 = (Comparable) o1[i];
          Comparable v2 = (Comparable) o2[i];
          int result = v1.compareTo(v2);
          if (result != 0) {
            return result * multipliers[i];
          }
        }
        return 0;
      };
    }
  }
}
