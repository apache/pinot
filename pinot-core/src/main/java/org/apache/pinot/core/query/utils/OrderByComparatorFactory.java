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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.data.table.Record;
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
        throw new BadQueryRequestException("MV expression: " + orderByExpressions.get(i)
            + " should not be included in the ORDER-BY clause");
      }
    }

    return getComparator(orderByExpressions, nullHandlingEnabled, from, to);
  }

  public static Comparator<Object[]> getComparator(List<OrderByExpressionContext> orderByExpressions,
      boolean nullHandlingEnabled) {
    return getComparator(orderByExpressions, nullHandlingEnabled, 0, orderByExpressions.size());
  }

  /**
   * get orderBy expressions on the groupBy keys when orderBy keys match groupBy keys
   */
  public static Comparator<Record> getRecordKeyComparator(List<OrderByExpressionContext> orderByExpressions,
      List<ExpressionContext> groupByExpressions, boolean nullHandlingEnabled) {
    List<OrderByExpressionWithIndex> groupKeyOrderByExpressions =
        getGroupKeyOrderByExpressionFromRowOrderByExpressions(orderByExpressions, groupByExpressions);
    Comparator<Object[]> valueComparator = getComparatorWithIndex(groupKeyOrderByExpressions, nullHandlingEnabled);
    return (k1, k2) -> valueComparator.compare(k1.getValues(), k2.getValues());
  }

  private static Map<String, Integer> getGroupByExpressionIndexMap(List<ExpressionContext> groupByExpressions) {
    Map<String, Integer> groupByExpressionIndexMap = new HashMap<>();
    int numGroupByExpressions = groupByExpressions.size();
    for (int i = 0; i < numGroupByExpressions; i++) {
      groupByExpressionIndexMap.put(groupByExpressions.get(i).getIdentifier(), i);
    }
    return groupByExpressionIndexMap;
  }

  /**
   * orderby expression with an index with respect to its position in the group keys
   */
  public static class OrderByExpressionWithIndex {
    OrderByExpressionContext _orderByExpressionContext;
    Integer _index;

    OrderByExpressionWithIndex(OrderByExpressionContext orderByExpressionContext, Integer index) {
      _orderByExpressionContext = orderByExpressionContext;
      _index = index;
    }
  }

  /**
   * add an index for each orderby expression with respect to its position in the group keys
   */
  public static List<OrderByExpressionWithIndex> getGroupKeyOrderByExpressionFromRowOrderByExpressions(
      List<OrderByExpressionContext> rowOrderByExpressions, List<ExpressionContext> groupByExpressions) {
    Map<String, Integer> groupByExpressionIndexMap = getGroupByExpressionIndexMap(groupByExpressions);
    List<OrderByExpressionWithIndex> result = new ArrayList<>();
    // get index wrt group key for each order by expression
    rowOrderByExpressions.forEach(expr ->
        result.add(
            new OrderByExpressionWithIndex(expr, groupByExpressionIndexMap.get(expr.getExpression().getIdentifier()))));
    return result;
  }

  /**
   * get comparator that applies list of orderByExpressions that each has a column index
   *
   * @param orderByExpressions
   * @param nullHandlingEnabled
   * @return
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static Comparator<Object[]> getComparatorWithIndex(List<OrderByExpressionWithIndex> orderByExpressions,
      boolean nullHandlingEnabled) {
    int expressionSize = orderByExpressions.size();

    // Use multiplier -1 or 1 to control ascending/descending order
    int[] multipliers = new int[expressionSize];
    // Use nulls multiplier -1 or 1 to control nulls last/first order
    int[] nullsMultipliers = new int[expressionSize];
    for (int i = 0; i < expressionSize; i++) {
      multipliers[i] = orderByExpressions.get(i)._orderByExpressionContext.isAsc() ? 1 : -1;
      nullsMultipliers[i] = orderByExpressions.get(i)._orderByExpressionContext.isNullsLast() ? 1 : -1;
    }
    if (nullHandlingEnabled) {
      return (Object[] o1, Object[] o2) -> {
        for (int i = 0; i < expressionSize; i++) {
          OrderByExpressionWithIndex expr = orderByExpressions.get(i);
          Comparable v1 = (Comparable) o1[expr._index];
          Comparable v2 = (Comparable) o2[expr._index];
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
        for (int i = 0; i < expressionSize; i++) {
          OrderByExpressionWithIndex expr = orderByExpressions.get(i);
          Comparable v1 = (Comparable) o1[expr._index];
          Comparable v2 = (Comparable) o2[expr._index];
          int result = v1.compareTo(v2);
          if (result != 0) {
            return result * multipliers[i];
          }
        }
        return 0;
      };
    }
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
