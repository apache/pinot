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
package org.apache.pinot.core.query.aggregation.groupby;

import com.google.common.collect.MinMaxPriorityQueue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.core.operator.GroupByRow;
import org.apache.pinot.core.operator.OrderByDefn;
import org.apache.pinot.core.operator.OrderByExecutor;


public class TrimmingService {

  private int _trimSize;
  private int _trimThreshold;
  private List<OrderByDefn> _orderByDefns = null;

  public TrimmingService(int interSegmentNumGroupsLimit, long topN, List<OrderByDefn> orderByDefns) {
    _trimSize = Math.max(Math.min(interSegmentNumGroupsLimit, (int) topN) * 5, 5000);
    _trimThreshold = _trimSize * 4;
    _orderByDefns = orderByDefns;
  }

  public List<GroupByRow> trim(List<GroupByRow> groupByRows) {

    // within threshold, no need to trim
    if (groupByRows.size() <= _trimThreshold) {
      return groupByRows;
    }

    // greater than trim threshold, needs trimming
    List<GroupByRow> trimmedRows = new ArrayList<>();
    if (_orderByDefns != null) {
      // if order by, sort by that order
      OrderByExecutor orderByExecutor = new OrderByExecutor();
      Comparator<GroupByRow> comparator = orderByExecutor.getComparator(_orderByDefns);

      MinMaxPriorityQueue<GroupByRow> priorityQueue =
          MinMaxPriorityQueue.orderedBy(comparator).maximumSize(_trimSize).create();
      for (GroupByRow row : groupByRows) {
        priorityQueue.add(row);
      }
      trimmedRows.addAll(priorityQueue);
    } else {
      trimmedRows = groupByRows.subList(0, _trimSize);
    }
    return trimmedRows;
  }
}
