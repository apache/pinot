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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.reduce.DataTableReducerContext;
import org.apache.pinot.core.query.reduce.GroupByReducerBase;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;


/**
 * Helper class to reduce data tables and set group by results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GroupByDataTableReducer extends GroupByReducerBase implements DataTableReducer {

  public GroupByDataTableReducer(QueryContext queryContext) {
    super(queryContext);
  }

  /**
   * Reduces and sets group by results into ResultTable, if responseFormat = sql
   * By default, sets group by results into GroupByResults
   */
  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    assert dataSchema != null;
    int resultSize = 0;
    Collection<DataTable> dataTables = dataTableMap.values();

    // For group by, PQL behavior is different than the SQL behavior. In the PQL way,
    // a result is generated for each aggregation in the query,
    // and the group by keys are not the same across the aggregations
    // This PQL style of execution makes it impossible to support order by on group by.
    //
    // We could not simply change the group by execution behavior,
    // as that would not be backward compatible for existing users of group by.
    // As a result, we have 2 modes of group by execution - pql and sql - which can be controlled via query options
    //
    // Long term, we may completely move to sql, and keep only full sql mode alive
    // Until then, we need to support responseFormat = sql for both the modes of execution.
    // The 4 variants are as described below:

    if (_groupByModeSql) {

      if (_responseFormatSql) {
        // 1. groupByMode = sql, responseFormat = sql
        // This is the primary SQL compliant group by

        try {
          setSQLGroupByInResultTable(brokerResponseNative, dataSchema, dataTables, reducerContext, tableName,
              brokerMetrics);
        } catch (TimeoutException e) {
          brokerResponseNative.getProcessingExceptions()
              .add(new QueryProcessingException(QueryException.BROKER_TIMEOUT_ERROR_CODE, e.getMessage()));
        }
        resultSize = brokerResponseNative.getResultTable().getRows().size();
      } else {
        // 2. groupByMode = sql, responseFormat = pql
        // This mode will invoke SQL style group by execution, but present results in PQL way
        // This mode is useful for users who want to avail of SQL compliant group by behavior,
        // w/o having to forcefully move to a new result type

        try {
          setSQLGroupByInAggregationResults(brokerResponseNative, dataSchema, dataTables, reducerContext);
        } catch (TimeoutException e) {
          brokerResponseNative.getProcessingExceptions()
              .add(new QueryProcessingException(QueryException.BROKER_TIMEOUT_ERROR_CODE, e.getMessage()));
        }

        if (!brokerResponseNative.getAggregationResults().isEmpty()) {
          resultSize = brokerResponseNative.getAggregationResults().get(0).getGroupByResult().size();
        }
      }
    } else {

      // 3. groupByMode = pql, responseFormat = sql
      // This mode is for users who want response presented in SQL style, but want PQL style group by behavior
      // Multiple aggregations in PQL violates the tabular nature of results
      // As a result, in this mode, only single aggregations are supported

      // 4. groupByMode = pql, responseFormat = pql
      // This is the primary PQL compliant group by

      setGroupByResults(brokerResponseNative, dataTables);

      if (_responseFormatSql) {
        resultSize = brokerResponseNative.getResultTable().getRows().size();
      } else {
        // We emit the group by size when the result isn't empty. All the sizes among group-by results should be the
        // same.
        // Thus, we can just emit the one from the 1st result.
        if (!brokerResponseNative.getAggregationResults().isEmpty()) {
          resultSize = brokerResponseNative.getAggregationResults().get(0).getGroupByResult().size();
        }
      }
    }

    if (brokerMetrics != null && resultSize > 0) {
      brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.GROUP_BY_SIZE, resultSize);
    }
  }
}
