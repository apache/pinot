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
package org.apache.pinot.core.query.reduce.datatable;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.reduce.AggregationReducerBase;
import org.apache.pinot.core.query.reduce.DataTableReducerContext;
import org.apache.pinot.core.query.reduce.PostAggregationHandler;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.QueryOptionsUtils;


/**
 * Helper class to reduce and set Aggregation results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationDataTableReducer extends AggregationReducerBase implements DataTableReducer {

  public AggregationDataTableReducer(QueryContext queryContext) {
    super(queryContext, queryContext.getAggregationFunctions());
    Map<String, String> queryOptions = queryContext.getQueryOptions();
    _preserveType = QueryOptionsUtils.isPreserveType(queryOptions);
    _responseFormatSql = QueryOptionsUtils.isResponseFormatSQL(queryOptions);
  }

  /**
   * Reduces data tables and sets aggregations results into
   * 1. ResultTable if _responseFormatSql is true
   * 2. AggregationResults by default
   */
  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    if (dataTableMap.isEmpty()) {
      if (_responseFormatSql) {
        DataSchema resultTableSchema =
            new PostAggregationHandler(_queryContext, getPrePostAggregationDataSchema()).getResultDataSchema();
        brokerResponseNative.setResultTable(new ResultTable(resultTableSchema, Collections.emptyList()));
      }
      return;
    }

    // Merge results from all data tables
    int numAggregationFunctions = _aggregationFunctions.length;
    Object[] intermediateResults = new Object[numAggregationFunctions];
    for (DataTable dataTable : dataTableMap.values()) {
      mergedResults(intermediateResults, dataSchema, dataTable);
    }
    Serializable[] finalResults = new Serializable[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      finalResults[i] = aggregationFunction.getFinalResultColumnType()
          .convert(aggregationFunction.extractFinalResult(intermediateResults[i]));
    }

    if (_responseFormatSql) {
      brokerResponseNative.setResultTable(reduceToResultTable(finalResults));
    } else {
      brokerResponseNative.setAggregationResults(reduceToAggregationResults(finalResults, dataSchema.getColumnNames()));
    }
  }
}
