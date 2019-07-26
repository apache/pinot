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
      comparator = Comparator.comparing(GroupByRow::getAggregationResults,
          Comparator.comparingDouble(s -> ((Number) s[index]).doubleValue()));
    } else if (orderType.equals(OrderType.GROUP_BY_KEY)) {
      comparator = Comparator.comparing(GroupByRow::getArrayKey, Comparator.comparing(s -> s[index]));
    }
    if (!orderByDefn.isAscending()) {
      comparator.reversed();
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
