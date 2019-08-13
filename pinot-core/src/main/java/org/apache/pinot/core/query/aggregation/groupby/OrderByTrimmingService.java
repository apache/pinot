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

import com.google.common.base.Preconditions;
import java.util.Comparator;
import java.util.List;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.GroupByRecord;
import org.apache.pinot.core.operator.OrderByUtils;


public class OrderByTrimmingService {

  private int _topN;

  private List<AggregationInfo> _aggregationInfos;
  private List<SelectionSort> _orderBy;
  private GroupBy _groupBy;
  private DataSchema _dataSchema;

  public OrderByTrimmingService(List<AggregationInfo> aggregationInfos, GroupBy groupBy, List<SelectionSort> orderBy,
      DataSchema dataSchema) {
    _aggregationInfos = aggregationInfos;
    _groupBy = groupBy;
    _orderBy = orderBy;
    _dataSchema = dataSchema;

    _topN = (int) groupBy.getTopN();
    Preconditions.checkArgument(_topN > 0);
  }

  public List<GroupByRecord> orderAndTrimIntermediate(List<GroupByRecord> groupByRecords) {
    // TODO: explain these trimmings
    int trimSize = Math.max(_topN * 5, 5000);
    int trimThreshold = trimSize * 4;

    // within threshold, no need to trim
    if (groupByRecords.size() <= trimThreshold) {
      return groupByRecords;
    }

    // order
    OrderByUtils orderByUtils = new OrderByUtils();
    Comparator<GroupByRecord> comparator =
        orderByUtils.getIntermediateComparator(_aggregationInfos, _groupBy, _orderBy, _dataSchema);
    // TODO: priority queue based approaches
    groupByRecords.sort(comparator);

    // trim
    return groupByRecords.subList(0, trimSize);
  }

  public List<GroupByRecord> orderAndTrimFinal(List<GroupByRecord> groupByRecords) {

    // order
    OrderByUtils orderByUtils = new OrderByUtils();
    Comparator<GroupByRecord> comparator =
        orderByUtils.getFinalComparator(_aggregationInfos, _groupBy, _orderBy, _dataSchema);
    // TODO: priority queue based approaches
    groupByRecords.sort(comparator);

    // trim
    int size = Math.min(_topN, groupByRecords.size());
    return groupByRecords.subList(0, size);
  }
}
