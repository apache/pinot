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

import org.apache.pinot.common.utils.EqualityUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;

import static org.apache.pinot.common.utils.DataSchema.*;


/**
 * The <code>OrderByInfo</code> class contains all information needed to execute an order by, given a list of {@link GroupByRecord}
 * An <code>OrderByInfo</code> will be constructed per expression in the order by clause
 */
public class OrderByInfo {

  /**
   * Given a {@link GroupByRecord} and {@link OrderType},
   * the index will indicate the position in the array this order by column holds.
   *
   * {@link OrderType::GROUP_BY_KEY} means we order by "index" position of groupByKeys[] array in the {@link GroupByRecord}
   * {@link OrderType::AGGREGATION_VALUE} means we order by "index" position of aggregationResults[] array in the {@link GroupByRecord}
   */
  private OrderType _orderType;
  private int _index;
  private boolean _ascending;
  private ColumnDataType _columnDataType;
  private AggregationFunction _aggregationFunction;

  public OrderByInfo(OrderType orderType, int index, boolean ascending, ColumnDataType columnDataType,
      AggregationFunction aggregationFunction) {
    _orderType = orderType;
    _index = index;
    _ascending = ascending;
    _columnDataType = columnDataType;
    _aggregationFunction = aggregationFunction;
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

  public AggregationFunction getAggregationFunction() {
    return _aggregationFunction;
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
        that._columnDataType) && EqualityUtils.isEqual(_aggregationFunction, that._aggregationFunction);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_orderType);
    result = EqualityUtils.hashCodeOf(result, _index);
    result = EqualityUtils.hashCodeOf(result, _ascending);
    result = EqualityUtils.hashCodeOf(result, _columnDataType);
    result = EqualityUtils.hashCodeOf(result, _aggregationFunction);
    return result;
  }
}
