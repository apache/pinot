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


/**
 * Helper methods to perform order by of list of {@link GroupByRecord}
 */
public class OrderByUtils {

  /**
   * Constructs the comparator to order the intermediate results
   */
  public static Comparator<GroupByRecord> getIntermediateComparator(List<AggregationInfo> aggregationInfos, GroupBy groupBy,
      List<SelectionSort> orderBy, DataSchema dataSchema) {
    List<OrderByInfo> orderByInfos = constructOrderByInfo(aggregationInfos, groupBy, orderBy, dataSchema);
    return getComparator(orderByInfos, true);
  }

  /**
   * Constructs the comparator to order the final results
   */
  public static Comparator<GroupByRecord> getFinalComparator(List<AggregationInfo> aggregationInfos, GroupBy groupBy,
      List<SelectionSort> orderBy, DataSchema dataSchema) {
    List<OrderByInfo> orderByInfos = constructOrderByInfo(aggregationInfos, groupBy, orderBy, dataSchema);
    return getComparator(orderByInfos, false);
  }

  /**
   * Constructs a comparator given all the {@link OrderByInfo} for the order by clause
   */
  private static Comparator<GroupByRecord> getComparator(List<OrderByInfo> orderByInfos, boolean isIntermediate) {
    Comparator<GroupByRecord> globalComparator = null;
    for (OrderByInfo orderByInfo : orderByInfos) {
      OrderType orderType = orderByInfo.getOrderType();

      Comparator<GroupByRecord> comparator;
      if (orderType.equals(OrderType.AGGREGATION_VALUE)) {
        comparator = getAggregationComparator(orderByInfo, isIntermediate);
      } else {
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

  /**
   * Constructs the comparator for ordering by an aggregation result
   *
   * When ordering intermediate results, check if the intermediate results are comparable.
   * If they are not, extract final result for doing the comparison
   */
  private static Comparator<GroupByRecord> getAggregationComparator(OrderByInfo orderByInfo, boolean isIntermediate) {
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

  /**
   * Constructs the comparator for ordering by a group by key
   */
  private static Comparator<GroupByRecord> getGroupByKeyComparator(OrderByInfo orderByInfo) {
    int index = orderByInfo.getIndex();
    ColumnDataType columnDataType = orderByInfo.getColumnDataType();
    boolean ascending = orderByInfo.isAscending();

    Comparator<GroupByRecord> comparator;

    switch (columnDataType) {
      case INT:
        if (ascending) {
          comparator = Comparator.comparing(GroupByRecord::getGroupByKey,
              Comparator.comparingInt(v -> Integer.valueOf(v[index])));
        } else {
          comparator = Comparator.comparing(GroupByRecord::getGroupByKey,
              (v1, v2) -> Integer.compare(Integer.valueOf(v2[index]), Integer.valueOf(v1[index])));
        }
        break;
      case LONG:
        if (ascending) {
          comparator =
              Comparator.comparing(GroupByRecord::getGroupByKey, Comparator.comparingLong(v -> Long.valueOf(v[index])));
        } else {
          comparator = Comparator.comparing(GroupByRecord::getGroupByKey,
              (v1, v2) -> Long.compare(Long.valueOf(v2[index]), Long.valueOf(v1[index])));
        }
        break;
      case FLOAT:
        if (ascending) {
          comparator =
              Comparator.comparing(GroupByRecord::getGroupByKey, Comparator.comparing(v -> Float.valueOf(v[index])));
        } else {
          comparator = Comparator.comparing(GroupByRecord::getGroupByKey,
              (v1, v2) -> Float.valueOf(v2[index]).compareTo(Float.valueOf(v1[index])));
        }
        break;
      case DOUBLE:
        if (ascending) {
          comparator = Comparator.comparing(GroupByRecord::getGroupByKey,
              Comparator.comparingDouble(v -> Double.valueOf(v[index])));
        } else {
          comparator = Comparator.comparing(GroupByRecord::getGroupByKey,
              (v1, v2) -> Double.compare(Double.valueOf(v2[index]), Double.valueOf(v1[index])));
        }
        break;
      case BYTES:
        if (ascending) {
          comparator = (v1, v2) -> ByteArray.compare(BytesUtils.toBytes(v1.getGroupByKey()[index]),
              BytesUtils.toBytes(v2.getGroupByKey()[index]));
        } else {
          comparator =
              (v1, v2) -> ByteArray.compare(v2.getGroupByKey()[index].getBytes(), v1.getGroupByKey()[index].getBytes());
        }
        break;
      case STRING:
      default:
        if (ascending) {
          comparator = Comparator.comparing(GroupByRecord::getGroupByKey, Comparator.comparing(v -> v[index]));
        } else {
          comparator = Comparator.comparing(GroupByRecord::getGroupByKey, (v1, v2) -> v2[index].compareTo(v1[index]));
        }
        break;
    }
    return comparator;
  }

  /**
   * Construct the list of {@link OrderByInfo} given the {@link AggregationInfo}, {@link GroupBy}, {@link SelectionSort} and {@link DataSchema}
   */
  private static List<OrderByInfo> constructOrderByInfo(List<AggregationInfo> aggregationInfos, GroupBy groupBy,
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
