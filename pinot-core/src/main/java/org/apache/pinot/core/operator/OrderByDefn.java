package org.apache.pinot.core.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.EqualityUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;


public class OrderByDefn {

  private OrderType _orderType; // from group by key, or from aggregation results
  private int _index; // the index, among all the group by keys or aggregation results
  private boolean _ascending;

  public OrderByDefn(OrderType orderType, int index, boolean ascending) {
    _orderType = orderType;
    _index = index;
    _ascending = ascending;
  }

  public OrderType getOrderType() {
    return _orderType;
  }

  public void setOrderType(OrderType orderType) {
    _orderType = orderType;
  }

  public int getIndex() {
    return _index;
  }

  public void setIndex(int index) {
    _index = index;
  }

  public boolean isAscending() {
    return _ascending;
  }

  public void setAscending(boolean ascending) {
    _ascending = ascending;
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    OrderByDefn that = (OrderByDefn) o;

    return EqualityUtils.isEqual(_index, that._index) && EqualityUtils.isEqual(_orderType, that._orderType)
        && EqualityUtils.isEqual(_ascending, that._ascending);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_orderType);
    result = EqualityUtils.hashCodeOf(result, _index);
    result = EqualityUtils.hashCodeOf(result, _ascending);
    return result;
  }

  public static boolean isAggregationsInOrderBy(List<OrderByDefn> orderByDefns) {
    return orderByDefns.stream().anyMatch(v -> v.getOrderType().equals(OrderType.AGGREGATION_VALUE));
  }

  public static List<OrderByDefn> getOrderByDefnsFromBrokerRequest(List<SelectionSort> orderByClause, GroupBy groupBy,
      List<AggregationInfo> aggregationInfos) {
    List<OrderByDefn> orderByDefns = new ArrayList<>(orderByClause.size());

    Map<String, Integer> groupByColumnToIndex = new HashMap<>(groupBy.getExpressionsSize());
    int index = 0;
    for (String groupByColumn : groupBy.getExpressions()) {
      groupByColumnToIndex.put(groupByColumn, index++);
    }

    Map<String, Integer> aggregationColumnToIndex = new HashMap<>(aggregationInfos.size());
    index = 0;
    for (AggregationInfo aggregationInfo : aggregationInfos) {
      aggregationColumnToIndex.put(
          aggregationInfo.getAggregationType().toLowerCase() + "(" + AggregationFunctionUtils.getColumn(aggregationInfo) + ")",
          index++);
    }

    for (SelectionSort orderBy : orderByClause) {
      boolean isAsc = orderBy.isIsAsc();
      String column = orderBy.getColumn();
      if (groupByColumnToIndex.containsKey(column)) {
        orderByDefns.add(new OrderByDefn(OrderType.GROUP_BY_KEY, groupByColumnToIndex.get(column), isAsc));
      } else if (aggregationColumnToIndex.containsKey(column)) {
        orderByDefns.add(new OrderByDefn(OrderType.AGGREGATION_VALUE, aggregationColumnToIndex.get(column), isAsc));
      } else {
        throw new UnsupportedOperationException(
            "Currently only support order by on group by columns or aggregations, already in query");
      }
    }

    return orderByDefns;
  }
}
