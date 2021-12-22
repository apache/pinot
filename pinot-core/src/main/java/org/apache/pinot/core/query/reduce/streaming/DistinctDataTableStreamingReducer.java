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
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.reduce.BaseDistinctReducer;
import org.apache.pinot.core.query.reduce.DataTableReducerContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;


/**
 * Helper class to reduce data tables and set results of distinct query into the BrokerResponseNative
 */
public class DistinctDataTableStreamingReducer extends BaseDistinctReducer implements StreamingReducer {

  private DistinctTable _mainDistinctTable;

  public DistinctDataTableStreamingReducer(QueryContext queryContext,
      DistinctAggregationFunction distinctAggregationFunction) {
    super(queryContext, distinctAggregationFunction);
  }

  @Override
  public void init(DataTableReducerContext dataTableReducerContext) {
  }

  /**
   * Reduces and sets results of distinct into
   * 1. ResultTable if _responseFormatSql is true
   * 2. SelectionResults by default
   */
  @Override
  public synchronized void reduce(ServerRoutingInstance key, DataTable dataTable) {
    // DISTINCT is implemented as an aggregation function in the execution engine. Just like
    // other aggregation functions, DISTINCT returns its result as a single object
    // (of type DistinctTable) serialized by the server into the DataTable and deserialized
    // by the broker from the DataTable. So there should be exactly 1 row and 1 column and that
    // column value should be the serialized DistinctTable -- so essentially it is a DataTable
    // inside a DataTable

    // Gather all non-empty DistinctTables
    DistinctTable distinctTable = dataTable.getObject(0, 0);
    if (!distinctTable.isEmpty()) {
      if (_mainDistinctTable == null) {
        // Construct a main DistinctTable and merge all non-empty DistinctTables into it
        _mainDistinctTable =
            new DistinctTable(distinctTable.getDataSchema(), _distinctAggregationFunction.getOrderByExpressions(),
                _distinctAggregationFunction.getLimit());
      }
      _mainDistinctTable.mergeTable(distinctTable);
    }
  }

  @Override
  public BrokerResponseNative seal() {
    // TODO: Handle case where reduce() is not called

    BrokerResponseNative brokerResponseNative = new BrokerResponseNative();
    // Up until now, we have treated DISTINCT similar to another aggregation function even in terms
    // of the result from function and merging results.
    // However, the DISTINCT query is just another SELECTION style query from the user's point
    // of view and will return one or records in the result table for the column(s) selected and so
    // for that reason, response from broker should be a selection query result.
    brokerResponseNative.setResultTable(reduceToResultTable(_mainDistinctTable));
    return brokerResponseNative;
  }
}
