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

  List<AggregationFunction> _aggregationFunctions;
  DataSchema _dataSchema;
  boolean _sort;

  int _maxCapacity;
  int _bufferedCapacity;

  @Override
  public void init(@Nonnull DataSchema dataSchema, List<AggregationInfo> aggregationInfos, List<SelectionSort> orderBy,
      int maxCapacity, boolean sort) {
    _dataSchema = dataSchema;
    _sort = sort;

    _aggregationFunctions = new ArrayList<>(aggregationInfos.size());
    for (AggregationInfo aggregationInfo : aggregationInfos) {
      _aggregationFunctions.add(
          AggregationFunctionUtils.getAggregationFunctionContext(aggregationInfo).getAggregationFunction());
    }

    /* Factor used to add buffer to maxCapacity of the table **/
    double bufferFactor;
    /* Factor used to decide eviction threshold **/
    /** The true capacity of the table is {@link IndexedTable::_bufferedCapacity},
     * which is bufferFactor times the {@link IndexedTable::_maxCapacity}
     *
     * If records beyond {@link IndexedTable::_bufferedCapacity} are received,
     * the table resize and evict bottom records, resizing it to {@link IndexedTable::_maxCapacity}
     * The assumption here is that {@link IndexedTable::_maxCapacity} already has a buffer added by the caller (typically, we do max(top * 5, 5000))
     */
    if (maxCapacity > 50000) {
      // if max capacity is large, buffer capacity is kept smaller, so that we do not accumulate too many records for sorting/resizing
      bufferFactor = 1.2;
    } else {
      // if max capacity is small, buffer capacity is kept larger, so that we avoid frequent resizing
      bufferFactor = 2.0;
    }
    _maxCapacity = maxCapacity;
    _bufferedCapacity = (int) (maxCapacity * bufferFactor);
  }
}
