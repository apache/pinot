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

import java.util.Iterator;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.SelectionSort;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;


/**
 * Base abstract implementation of Table for indexed lookup
 */
public abstract class IndexedTable implements Table {

  AggregationFunction[] _aggregationFunctions;
  int _numAggregations;
  private DataSchema _dataSchema;

  int _capacity;
  int _maxCapacity;

  boolean _isOrderBy;
  IndexedTableResizer _indexedTableResizer;

  /**
   * Initializes the variables and comparators needed for the table
   */
  public IndexedTable(DataSchema dataSchema, List<AggregationInfo> aggregationInfos, List<SelectionSort> orderBy,
      int capacity) {
    _dataSchema = dataSchema;

    _numAggregations = aggregationInfos.size();
    _aggregationFunctions = new AggregationFunction[_numAggregations];
    for (int i = 0; i < _numAggregations; i++) {
      _aggregationFunctions[i] =
          AggregationFunctionUtils.getAggregationFunctionContext(aggregationInfos.get(i)).getAggregationFunction();
    }

    _isOrderBy = CollectionUtils.isNotEmpty(orderBy);
    if (_isOrderBy) {
      _indexedTableResizer = new IndexedTableResizer(dataSchema, aggregationInfos, orderBy);

      // TODO: tune these numbers
      // Based on the capacity and maxCapacity, the resizer will smartly choose to evict/retain recors from the PQ
      if (capacity <= 100_000) { // Capacity is small, make a very large buffer. Make PQ of records to retain, during resize
        _maxCapacity = 1_000_000;
      } else { // Capacity is large, make buffer only slightly bigger. Make PQ of records to evict, during resize
        _maxCapacity = (int) (capacity * 1.2);
      }
    } else {
      _maxCapacity = capacity;
    }
    _capacity = capacity;
  }

  @Override
  public boolean merge(Table table) {
    Iterator<Record> iterator = table.iterator();
    while (iterator.hasNext()) {
      upsert(iterator.next());
    }
    return true;
  }

  @Override
  public DataSchema getDataSchema() {
    return _dataSchema;
  }
}
