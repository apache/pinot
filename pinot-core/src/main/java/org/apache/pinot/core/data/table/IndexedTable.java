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

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Base implementation of Map-based Table for indexed lookup
 */
@SuppressWarnings("rawtypes")
public abstract class IndexedTable extends BaseTable {
  protected final int _numKeyColumns;
  protected final AggregationFunction[] _aggregationFunctions;
  protected final boolean _hasOrderBy;
  protected final TableResizer _tableResizer;

  // The capacity we need to trim to
  protected final int _capacity;
  // The capacity with added buffer, in order to collect more records than capacity for better precision
  protected final int _maxCapacity;

  protected IndexedTable(DataSchema dataSchema, QueryContext queryContext, int capacity) {
    super(dataSchema);

    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert groupByExpressions != null;
    _numKeyColumns = groupByExpressions.size();

    _aggregationFunctions = queryContext.getAggregationFunctions();

    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    if (orderByExpressions != null) {
      _hasOrderBy = true;
      _tableResizer = new TableResizer(dataSchema, queryContext);
      _capacity = capacity;

      // TODO: tune these numbers and come up with a better formula (github ISSUE-4801)
      // Based on the capacity and maxCapacity, the resizer will smartly choose to evict/retain recors from the PQ
      if (capacity
          <= 100_000) { // Capacity is small, make a very large buffer. Make PQ of records to retain, during resize
        _maxCapacity = 1_000_000;
      } else { // Capacity is large, make buffer only slightly bigger. Make PQ of records to evict, during resize
        _maxCapacity = (int) (capacity * 1.2);
      }
    } else {
      _hasOrderBy = false;
      _tableResizer = null;
      _capacity = capacity;
      _maxCapacity = capacity;
    }
  }

  @Override
  public boolean upsert(Record record) {
    // NOTE: The record will always have key columns (group-by expressions) in the front. This is handled in
    //       AggregationGroupByOrderByOperator.
    Object[] keyValues = Arrays.copyOf(record.getValues(), _numKeyColumns);
    return upsert(new Key(keyValues), record);
  }
}
