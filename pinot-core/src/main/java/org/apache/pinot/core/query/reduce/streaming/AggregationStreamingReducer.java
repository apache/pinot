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

import java.io.Serializable;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.reduce.BaseAggregationReducer;
import org.apache.pinot.core.query.reduce.DataTableReducerContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;


/**
 * Helper class to reduce and set Aggregation results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationStreamingReducer extends BaseAggregationReducer implements StreamingReducer {
  private final Object[] _intermediateResults;

  private DataSchema _dataSchema;

  public AggregationStreamingReducer(QueryContext queryContext) {
    super(queryContext);

    _intermediateResults = new Object[_aggregationFunctions.length];
  }

  @Override
  public void init(DataTableReducerContext dataTableReducerContext) {
  }

  /**
   * Reduces data tables and sets aggregations results into
   * 1. ResultTable if _responseFormatSql is true
   * 2. AggregationResults by default
   */
  @Override
  public synchronized void reduce(ServerRoutingInstance key, DataTable dataTable) {
    if (_dataSchema == null) {
      _dataSchema = dataTable.getDataSchema();
    }

    // Merge results from all data tables
    mergedResults(_intermediateResults, _dataSchema, dataTable);
  }

  @Override
  public BrokerResponseNative seal() {
    // TODO: Handle case where reduce() is not called

    Serializable[] finalResults = new Serializable[_aggregationFunctions.length];
    for (int i = 0; i < _aggregationFunctions.length; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      finalResults[i] = aggregationFunction.getFinalResultColumnType()
          .convert(aggregationFunction.extractFinalResult(_intermediateResults[i]));
    }

    BrokerResponseNative brokerResponseNative = new BrokerResponseNative();
    brokerResponseNative.setResultTable(reduceToResultTable(finalResults));
    return brokerResponseNative;
  }
}
