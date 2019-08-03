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
package org.apache.pinot.core.operator;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class OrderByExecutor {

  private Comparator<GroupByRow> getComparator(OrderByDefn orderByDefn) {
    OrderType orderType = orderByDefn.getOrderType();
    int index = orderByDefn.getIndex();

    Comparator<GroupByRow> comparator = null;
    if (orderType.equals(OrderType.AGGREGATION_VALUE)) {
      if (orderByDefn.isAscending()) {
        comparator = Comparator.comparing(GroupByRow::getAggregationResults,
            Comparator.comparingDouble(s -> ((Number) s[index]).doubleValue())); // TODO: other data types?
      } else {
        comparator = Comparator.comparing(GroupByRow::getAggregationResults,
            (s1, s2) -> Double.compare(((Number) s2[index]).doubleValue(),
                ((Number) s1[index]).doubleValue())); // TODO: other data types?
      }
    } else if (orderType.equals(OrderType.GROUP_BY_KEY)) {
      if (orderByDefn.isAscending()) {
        comparator = Comparator.comparing(GroupByRow::getArrayKey, Comparator.comparing(s -> s[index]));
      } else {
        comparator = Comparator.comparing(GroupByRow::getArrayKey, (s1, s2) -> s2[index].compareTo(s1[index]));
      }
    }
    return comparator;
  }

  public Comparator<GroupByRow> getComparator(List<OrderByDefn> orderByDefns) {
    Comparator<GroupByRow> globalComparator = null;
    for (OrderByDefn orderByDefn : orderByDefns) {
      Comparator<GroupByRow> comparator = getComparator(orderByDefn);
      if (globalComparator == null) {
        globalComparator = comparator;
      } else {
        globalComparator = globalComparator.thenComparing(comparator);
      }
    }
    return globalComparator;
  }

  public void sort(List<GroupByRow> groupByRows, List<OrderByDefn> orderByDefns) {
    Comparator<GroupByRow> globalComparator = getComparator(orderByDefns);
    Collections.sort(groupByRows, globalComparator);
  }
}
