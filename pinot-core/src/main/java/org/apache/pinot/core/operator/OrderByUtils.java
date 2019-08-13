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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.primitive.ByteArray;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;

import static org.apache.pinot.common.utils.DataSchema.*;


public class OrderByUtils {

  public Comparator<GroupByRecord> getIntermediateComparator(List<AggregationInfo> aggregationInfos, GroupBy groupBy,
      List<SelectionSort> orderBy, DataSchema dataSchema) {
    List<OrderByInfo> orderByInfos = constructOrderByInfo(aggregationInfos, groupBy, orderBy, dataSchema);
    return getComparator(orderByInfos, true);
  }

  public Comparator<GroupByRecord> getFinalComparator(List<AggregationInfo> aggregationInfos, GroupBy groupBy,
      List<SelectionSort> orderBy, DataSchema dataSchema) {
    List<OrderByInfo> orderByInfos = constructOrderByInfo(aggregationInfos, groupBy, orderBy, dataSchema);
    return getComparator(orderByInfos, false);
  }

  private Comparator<GroupByRecord> getComparator(List<OrderByInfo> orderByInfos, boolean isIntermediate) {
    Comparator<GroupByRecord> globalComparator = null;
    for (OrderByInfo orderByInfo : orderByInfos) {
      OrderType orderType = orderByInfo.getOrderType();

      Comparator<GroupByRecord> comparator = null;
      if (orderType.equals(OrderType.AGGREGATION_VALUE)) {
        comparator = getAggregationComparator(orderByInfo, isIntermediate);
      } else if (orderType.equals(OrderType.GROUP_BY_KEY)) {
        comparator = getGroupByKeyComparator(orderByInfo);
      }
      if (globalComparator == null) {
        globalComparator = comparator;
      } else {
        globalComparator = globalComparator.thenComparing(comparator);
      }
    }
    return globalComparator;
  }

  private Comparator<GroupByRecord> getAggregationComparator(OrderByInfo orderByInfo, boolean isIntermediate) {
    int index = orderByInfo.getIndex();
    AggregationFunction aggregationFunction = orderByInfo.getAggregationFunction();
    boolean ascending = orderByInfo.isAscending();

    Comparator<GroupByRecord> comparator;

    // intermediate aggregation results may or may not be comparable
    // final aggregation results are always comparable
    if (isIntermediate && !aggregationFunction.isIntermediateResultComparable()) {
      if (ascending) {
        comparator = (v1, v2) -> ComparableComparator.getInstance()
            .compare(aggregationFunction.extractFinalResult(v1.getAggregationResults()[index]),
                aggregationFunction.extractFinalResult(v2.getAggregationResults()[index]));
      } else {
        comparator = (v1, v2) -> ComparableComparator.getInstance()
            .compare(aggregationFunction.extractFinalResult(v2.getAggregationResults()[index]),
                aggregationFunction.extractFinalResult(v1.getAggregationResults()[index]));
      }
    } else {
      if (ascending) {
        comparator = (v1, v2) -> ComparableComparator.getInstance()
            .compare(v1.getAggregationResults()[index], v2.getAggregationResults()[index]);
      } else {
        comparator = (v1, v2) -> ComparableComparator.getInstance()
            .compare(v2.getAggregationResults()[index], v1.getAggregationResults()[index]);
      }
    }
    return comparator;
  }

  private Comparator<GroupByRecord> getGroupByKeyComparator(OrderByInfo orderByInfo) {
    int index = orderByInfo.getIndex();
    ColumnDataType columnDataType = orderByInfo.getColumnDataType();
    boolean ascending = orderByInfo.isAscending();

    Comparator<GroupByRecord> comparator;

    switch (columnDataType) {
      case INT:
        if (ascending) {
          comparator =
              Comparator.comparing(GroupByRecord::getArrayKey, Comparator.comparingInt(v -> Integer.valueOf(v[index])));
        } else {
          comparator = Comparator.comparing(GroupByRecord::getArrayKey,
              (v1, v2) -> Integer.compare(Integer.valueOf(v2[index]), Integer.valueOf(v1[index])));
        }
        break;
      case LONG:
        if (ascending) {
          comparator =
              Comparator.comparing(GroupByRecord::getArrayKey, Comparator.comparingLong(v -> Long.valueOf(v[index])));
        } else {
          comparator = Comparator.comparing(GroupByRecord::getArrayKey,
              (v1, v2) -> Long.compare(Long.valueOf(v2[index]), Long.valueOf(v1[index])));
        }
        break;
      case FLOAT:
        if (ascending) {
          comparator =
              Comparator.comparing(GroupByRecord::getArrayKey, Comparator.comparing(v -> Float.valueOf(v[index])));
        } else {
          comparator = Comparator.comparing(GroupByRecord::getArrayKey,
              (v1, v2) -> Float.valueOf(v2[index]).compareTo(Float.valueOf(v1[index])));
        }
        break;
      case DOUBLE:
        if (ascending) {
          comparator = Comparator.comparing(GroupByRecord::getArrayKey,
              Comparator.comparingDouble(v -> Double.valueOf(v[index])));
        } else {
          comparator = Comparator.comparing(GroupByRecord::getArrayKey,
              (v1, v2) -> Double.compare(Double.valueOf(v2[index]), Double.valueOf(v1[index])));
        }
        break;
      case BYTES:
        if (ascending) {
          comparator = (v1, v2) -> ByteArray.compare(BytesUtils.toBytes(v1.getArrayKey()[index]),
              BytesUtils.toBytes(v2.getArrayKey()[index]));
        } else {
          comparator =
              (v1, v2) -> ByteArray.compare(v2.getArrayKey()[index].getBytes(), v1.getArrayKey()[index].getBytes());
        }
        break;
      case STRING:
      default:
        if (ascending) {
          comparator = Comparator.comparing(GroupByRecord::getArrayKey, Comparator.comparing(v -> v[index]));
        } else {
          comparator = Comparator.comparing(GroupByRecord::getArrayKey, (v1, v2) -> v2[index].compareTo(v1[index]));
        }
        break;
    }
    return comparator;
  }

  private List<OrderByInfo> constructOrderByInfo(List<AggregationInfo> aggregationInfos, GroupBy groupBy,
      List<SelectionSort> orderByClause, DataSchema dataSchema) {
    List<OrderByInfo> orderByInfos = new ArrayList<>(orderByClause.size());

    Map<String, Integer> groupByColumnToIndex = new HashMap<>(groupBy.getExpressionsSize());
    int index = 0;
    for (String groupByColumn : groupBy.getExpressions()) {
      groupByColumnToIndex.put(groupByColumn, index++);
    }

    Map<String, Integer> aggregationColumnToIndex = new HashMap<>(aggregationInfos.size());
    Map<String, AggregationInfo> aggregationColumnToInfo = new HashMap<>(aggregationInfos.size());
    index = 0;
    for (AggregationInfo aggregationInfo : aggregationInfos) {
      String aggregationColumn =
          aggregationInfo.getAggregationType().toLowerCase() + "(" + AggregationFunctionUtils.getColumn(aggregationInfo)
              + ")";
      aggregationColumnToIndex.put(aggregationColumn, index++);
      aggregationColumnToInfo.put(aggregationColumn, aggregationInfo);
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
            new OrderByInfo(OrderType.GROUP_BY_KEY, groupByColumnToIndex.get(column), isAsc, columnDataType, null));
      } else if (aggregationColumnToIndex.containsKey(column)) {
        AggregationFunctionContext aggregationFunctionContext =
            AggregationFunctionUtils.getAggregationFunctionContext(aggregationColumnToInfo.get(column));
        orderByInfos.add(
            new OrderByInfo(OrderType.AGGREGATION_VALUE, aggregationColumnToIndex.get(column), isAsc, columnDataType,
                aggregationFunctionContext.getAggregationFunction()));
      } else {
        throw new UnsupportedOperationException(
            "Currently only support order by on group by columns or aggregations, already in query");
      }
    }
    return orderByInfos;
  }
}
