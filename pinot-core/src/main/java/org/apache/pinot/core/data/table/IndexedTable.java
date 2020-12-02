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
  protected List<Record> _sortedRecords;
  // The size we need to trim to
  protected final int _trimSize;
  // The size with added buffer, in order to collect more records than capacity for better precision
  protected final int _trimThreshold;

  protected IndexedTable(DataSchema dataSchema, QueryContext queryContext, int trimSize, int trimThreshold) {
    super(dataSchema);
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert groupByExpressions != null;
    _numKeyColumns = groupByExpressions.size();
    _aggregationFunctions = queryContext.getAggregationFunctions();
    List<OrderByExpressionContext> orderByExpressions = queryContext.getOrderByExpressions();
    if (orderByExpressions != null) {
      // SQL GROUP BY with ORDER BY
      // trimSize = max (limit N * 5, 5000) (see GroupByUtils.getTableCapacity).
      // trimSize is also bound by trimThreshold/2 to protect the server in case
      // when user specifies a very high value of LIMIT N.
      // trimThreshold is configurable. to keep parity with PQL for some use
      // cases with infinitely large group by, trimThreshold will be >= 1B
      // (exactly same as PQL). This essentially implies there will be no
      // resizing/trimming during upsert and exactly one trim during finish.
      _hasOrderBy = true;
      _tableResizer = new TableResizer(dataSchema, queryContext);
      _trimSize = Math.min(trimSize, trimThreshold / 2);
      _trimThreshold = trimThreshold;
    } else {
      // SQL GROUP BY without ORDER BY
      // trimSize = LIMIT N (see GroupByUtils.getTableCapacity)
      // trimThreshold is same as trimSize since indexed table stops
      // accepting records once map size reaches trimSize. there is no
      // resize/trim during upsert since the results can be arbitrary
      // and are truncated once they reach trimSize
      _hasOrderBy = false;
      _tableResizer = null;
      _trimSize = trimSize;
      _trimThreshold = trimSize;
    }
  }

  @Override
  public boolean upsert(Record record) {
    // NOTE: The record will always have key columns (group-by expressions) in the front. This is handled in
    //       AggregationGroupByOrderByOperator.
    Object[] keyValues = Arrays.copyOf(record.getValues(), _numKeyColumns);
    return upsert(new Key(keyValues), record);
  }

  public abstract int getNumResizes();

  public abstract long getResizeTimeMs();
}
