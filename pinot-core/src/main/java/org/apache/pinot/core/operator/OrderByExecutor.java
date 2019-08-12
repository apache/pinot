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
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.common.utils.primitive.ByteArray;

import static org.apache.pinot.common.utils.DataSchema.*;


public class OrderByExecutor {

  private Comparator<GroupByRecord> getComparator(OrderByInfo orderByInfo) {
    OrderType orderType = orderByInfo.getOrderType();
    int index = orderByInfo.getIndex();
    ColumnDataType columnDataType = orderByInfo.getColumnDataType();

    Comparator<GroupByRecord> comparator = null;
    if (orderType.equals(OrderType.AGGREGATION_VALUE)) {
      if (orderByInfo.isAscending()) {
        comparator = Comparator.comparing(GroupByRecord::getAggregationResults,
            Comparator.comparingDouble(s -> ((Number) s[index]).doubleValue())); // TODO: other data types?
      } else {
        comparator = Comparator.comparing(GroupByRecord::getAggregationResults,
            (s1, s2) -> Double.compare(((Number) s2[index]).doubleValue(),
                ((Number) s1[index]).doubleValue())); // TODO: other data types?
      }
    } else if (orderType.equals(OrderType.GROUP_BY_KEY)) {
      switch (columnDataType) {
        case INT:
          if (orderByInfo.isAscending()) {
            comparator = Comparator.comparing(GroupByRecord::getArrayKey,
                Comparator.comparingInt(s -> Integer.valueOf(s[index])));
          } else {
            comparator = Comparator.comparing(GroupByRecord::getArrayKey,
                (s1, s2) -> Integer.compare(Integer.valueOf(s2[index]), Integer.valueOf(s1[index])));
          }
          break;
        case LONG:
          if (orderByInfo.isAscending()) {
            comparator = Comparator.comparing(GroupByRecord::getArrayKey,
                Comparator.comparingLong(s -> Long.valueOf(s[index])));
          } else {
            comparator = Comparator.comparing(GroupByRecord::getArrayKey,
                (s1, s2) -> Long.compare(Long.valueOf(s2[index]), Long.valueOf(s1[index])));
          }
          break;
        case FLOAT:
          if (orderByInfo.isAscending()) {
            comparator = Comparator.comparing(GroupByRecord::getArrayKey,
                Comparator.comparing(s -> Float.valueOf(s[index])));
          } else {
            comparator = Comparator.comparing(GroupByRecord::getArrayKey,
                (s1, s2) -> Float.valueOf(s2[index]).compareTo(Float.valueOf(s1[index])));
          }
          break;
        case DOUBLE:
          if (orderByInfo.isAscending()) {
            comparator = Comparator.comparing(GroupByRecord::getArrayKey,
                Comparator.comparingDouble(s -> Double.valueOf(s[index])));
          } else {
            comparator = Comparator.comparing(GroupByRecord::getArrayKey,
                (s1, s2) -> Double.compare(Double.valueOf(s2[index]), Double.valueOf(s1[index])));
          }
          break;
        case BYTES:
          if (orderByInfo.isAscending()) {
            comparator = (o1, o2) -> ByteArray.compare(BytesUtils.toBytes(o1.getArrayKey()[index]),
                BytesUtils.toBytes(o2.getArrayKey()[index]));
          } else {
            comparator =
                (o1, o2) -> ByteArray.compare(o2.getArrayKey()[index].getBytes(), o1.getArrayKey()[index].getBytes());
          }
          break;
        case STRING:
        default:
          if (orderByInfo.isAscending()) {
            comparator = Comparator.comparing(GroupByRecord::getArrayKey, Comparator.comparing(s -> s[index]));
          } else {
            comparator = Comparator.comparing(GroupByRecord::getArrayKey, (s1, s2) -> s2[index].compareTo(s1[index]));
          }
          break;
      }
    }
    return comparator;
  }

  public Comparator<GroupByRecord> getComparator(List<OrderByInfo> orderByInfos) {
    Comparator<GroupByRecord> globalComparator = null;
    for (int i = 0; i < orderByInfos.size(); i++) {
      Comparator<GroupByRecord> comparator = getComparator(orderByInfos.get(i));
      if (globalComparator == null) {
        globalComparator = comparator;
      } else {
        globalComparator = globalComparator.thenComparing(comparator);
      }
    }
    return globalComparator;
  }

  public void sort(List<GroupByRecord> groupByRecords, List<OrderByInfo> orderByInfos) {
    Comparator<GroupByRecord> globalComparator = getComparator(orderByInfos);
    Collections.sort(groupByRecords, globalComparator);
  }
}
