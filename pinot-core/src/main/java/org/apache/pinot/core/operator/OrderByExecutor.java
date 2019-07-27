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
