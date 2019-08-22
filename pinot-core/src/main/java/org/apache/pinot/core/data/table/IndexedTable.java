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
package org.apache.pinot.core.data.table;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;


/**
 * Base abstract implementation of Table for indexed lookup
 */
public abstract class IndexedTable implements Table {
  // When table reaches max capacity, we will allow 20% more records to get inserted (bufferedCapacity)
  // If records beyond bufferedCapacity are received, the table will undergo sort and evict upto evictCapacity (10% more than capacity)
  // This is to ensure that for a small number beyond capacity, a fair chance is given to all records which have the potential to climb up the order
  /** Factor used to add buffer to maxCapacity of the Collection used **/
  private static final double BUFFER_FACTOR = 1.2;
  /** Factor used to decide eviction threshold **/
  private static final double EVICTION_FACTOR = 1.1;

  private List<AggregationFunction> _aggregationFunctions;
  DataSchema _dataSchema;
  List<AggregationInfo> _aggregationInfos;
  List<SelectionSort> _orderBy;

  int _maxCapacity;
  int _evictCapacity;
  int _bufferedCapacity;

  @Override
  public void init(@Nonnull DataSchema dataSchema, List<AggregationInfo> aggregationInfos, List<SelectionSort> orderBy,
      int maxCapacity) {
    _dataSchema = dataSchema;
    _aggregationInfos = aggregationInfos;
    _orderBy = orderBy;

    _aggregationFunctions = new ArrayList<>(aggregationInfos.size());
    for (AggregationInfo aggregationInfo : aggregationInfos) {
      _aggregationFunctions.add(
          AggregationFunctionUtils.getAggregationFunctionContext(aggregationInfo).getAggregationFunction());
    }

    _maxCapacity = maxCapacity;
    _bufferedCapacity = (int) (maxCapacity * BUFFER_FACTOR);
    _evictCapacity = (int) (maxCapacity * EVICTION_FACTOR);
  }

  void aggregate(Record existingRecord, Record newRecord) {
    for (int i = 0; i < _aggregationFunctions.size(); i++) {
      existingRecord.getValues()[i] =
          _aggregationFunctions.get(i).merge(existingRecord.getValues()[i], newRecord.getValues()[i]);
    }
  }

}
