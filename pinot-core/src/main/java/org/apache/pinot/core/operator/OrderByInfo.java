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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.EqualityUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;

import static org.apache.pinot.common.utils.DataSchema.*;


public class OrderByInfo {

  private OrderType _orderType; // from group by key, or from aggregation results
  private int _index; // the index, among all the group by keys or aggregation results
  private boolean _ascending;
  private ColumnDataType _columnDataType;

  public OrderByInfo(OrderType orderType, int index, boolean ascending, ColumnDataType columnDataType) {
    _orderType = orderType;
    _index = index;
    _ascending = ascending;
    _columnDataType = columnDataType;
  }

  public OrderType getOrderType() {
    return _orderType;
  }

  public int getIndex() {
    return _index;
  }

  public boolean isAscending() {
    return _ascending;
  }

  public ColumnDataType getColumnDataType() {
    return _columnDataType;
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    OrderByInfo that = (OrderByInfo) o;

    return EqualityUtils.isEqual(_index, that._index) && EqualityUtils.isEqual(_orderType, that._orderType)
        && EqualityUtils.isEqual(_ascending, that._ascending) && EqualityUtils.isEqual(_columnDataType,
        that._columnDataType);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_orderType);
    result = EqualityUtils.hashCodeOf(result, _index);
    result = EqualityUtils.hashCodeOf(result, _ascending);
    result = EqualityUtils.hashCodeOf(result, _columnDataType);
    return result;
  }

  public static List<OrderByInfo> getOrderByInfoFromBrokerRequest(List<SelectionSort> orderByClause, GroupBy groupBy,
      List<AggregationInfo> aggregationInfos, DataSchema dataSchema) {
    List<OrderByInfo> orderByInfos = new ArrayList<>(orderByClause.size());

    Map<String, Integer> groupByColumnToIndex = new HashMap<>(groupBy.getExpressionsSize());
    int index = 0;
    for (String groupByColumn : groupBy.getExpressions()) {
      groupByColumnToIndex.put(groupByColumn, index++);
    }

    Map<String, Integer> aggregationColumnToIndex = new HashMap<>(aggregationInfos.size());
    index = 0;
    for (AggregationInfo aggregationInfo : aggregationInfos) {
      aggregationColumnToIndex.put(
          aggregationInfo.getAggregationType().toLowerCase() + "(" + AggregationFunctionUtils.getColumn(aggregationInfo)
              + ")", index++);
    }

    Map<String, ColumnDataType> columnDataTypeMap = new HashMap<>(dataSchema.size());
    for (int i = 0; i < dataSchema.size(); i++) {
      columnDataTypeMap.put(dataSchema.getColumnName(i), dataSchema.getColumnDataType(i));
    }

    for (SelectionSort orderBy : orderByClause) {
      boolean isAsc = orderBy.isIsAsc();
      String column = orderBy.getColumn();
      ColumnDataType columnDataType = columnDataTypeMap.get(column);
      if (groupByColumnToIndex.containsKey(column)) {
        orderByInfos.add(
            new OrderByInfo(OrderType.GROUP_BY_KEY, groupByColumnToIndex.get(column), isAsc, columnDataType));
      } else if (aggregationColumnToIndex.containsKey(column)) {
        orderByInfos.add(
            new OrderByInfo(OrderType.AGGREGATION_VALUE, aggregationColumnToIndex.get(column), isAsc, columnDataType));
      } else {
        throw new UnsupportedOperationException(
            "Currently only support order by on group by columns or aggregations, already in query");
      }
    }

    return orderByInfos;
  }
}
