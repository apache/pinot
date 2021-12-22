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
package org.apache.pinot.core.query.reduce.streaming;

import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.query.reduce.BaseGroupByReducer;
import org.apache.pinot.core.query.reduce.DataTableReducerContext;
import org.apache.pinot.core.query.reduce.ReducerUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GroupByUtils;


/**
 * Helper class to reduce data tables and set group by results into the BrokerResponseNative
 */
public class GroupByDataTableStreamingReducer extends BaseGroupByReducer implements StreamingReducer {
  private DataTableReducerContext _reducerContext;
  private volatile IndexedTable _indexedTable;

  public GroupByDataTableStreamingReducer(QueryContext queryContext) {
    super(queryContext);
  }

  @Override
  public void init(DataTableReducerContext dataTableReducerContext) {
    _reducerContext = dataTableReducerContext;
  }

  /**
   * Reduces and sets group by results into ResultTable, if responseFormat = sql
   * By default, sets group by results into GroupByResults
   */
  @Override
  public void reduce(ServerRoutingInstance key, DataTable dataTable) {
    int limit = _queryContext.getLimit();
    // TODO: Make minTrimSize configurable
    int trimSize = GroupByUtils.getTableCapacity(limit);
    // NOTE: For query with HAVING clause, use trimSize as resultSize to ensure the result accuracy.
    // TODO: Resolve the HAVING clause within the IndexedTable before returning the result
    int resultSize = _queryContext.getHavingFilter() != null ? trimSize : limit;
    int trimThreshold = _reducerContext.getGroupByTrimThreshold();

    IndexedTable indexedTable = _indexedTable;
    if (indexedTable == null) {
      synchronized (this) {
        indexedTable = _indexedTable;
        if (indexedTable == null) {
          indexedTable =
              new ConcurrentIndexedTable(dataTable.getDataSchema(), _queryContext, resultSize, trimSize, trimThreshold);
          _indexedTable = indexedTable;
        }
      }
    }

    int numRows = dataTable.getNumberOfRows();
    ColumnDataType[] storedColumnDataTypes = dataTable.getDataSchema().getStoredColumnDataTypes();
    for (int rowId = 0; rowId < numRows; rowId++) {
      indexedTable.upsert(ReducerUtils.getRecord(dataTable, rowId, storedColumnDataTypes));
    }
  }

  @Override
  public BrokerResponseNative seal() {
    // TODO: Handle case where reduce() is not called

    BrokerResponseNative brokerResponseNative = new BrokerResponseNative();
    brokerResponseNative.setResultTable(getResultTable(_indexedTable));
    return brokerResponseNative;
  }
}
