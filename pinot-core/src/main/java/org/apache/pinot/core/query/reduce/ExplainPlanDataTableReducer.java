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
package org.apache.pinot.core.query.reduce;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.operator.filter.EmptyFilterOperator;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExplainPlanDataTableReducer implements DataTableReducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExplainPlanDataTableReducer.class);

  private final QueryContext _queryContext;

  ExplainPlanDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
  }

  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {

    Map.Entry<ServerRoutingInstance, DataTable> entry = selectBestServerDataTableResultToUse(dataTableMap);
    DataTable dataTable = entry.getValue();
    List<Object[]> reducedRows = new ArrayList<>();

    // Top node should be a BROKER_REDUCE node.
    addBrokerReduceOperation(reducedRows);

    // Add rest of the rows received from the server.
    int numRows = dataTable.getNumberOfRows();
    for (int rowId = 0; rowId < numRows; rowId++) {
      reducedRows.add(SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId));
    }

    ResultTable resultTable = new ResultTable(dataSchema, reducedRows);
    brokerResponseNative.setResultTable(resultTable);
  }

  private Map.Entry<ServerRoutingInstance, DataTable> selectBestServerDataTableResultToUse(
      Map<ServerRoutingInstance, DataTable> dataTableMap) {
    int maxOtherDepth = -1;
    int maxEmptyFilterDepth = -1;
    int maxMatchAllFilterDepth = -1;
    Map.Entry<ServerRoutingInstance, DataTable> maxOtherEntry = null;
    Map.Entry<ServerRoutingInstance, DataTable> maxEmptyFilterEntry = null;
    Map.Entry<ServerRoutingInstance, DataTable> maxMatchAllFilterEntry = null;
    for (Map.Entry<ServerRoutingInstance, DataTable> entry : dataTableMap.entrySet()) {
      DataTable dataTable = entry.getValue();
      boolean hasEmptyFilter = false;
      boolean hasMatchAllFilter = false;
      int numRows = dataTable.getNumberOfRows();

      for (int rowId = 0; rowId < numRows; rowId++) {
        Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
        if (row[0].toString().contains(EmptyFilterOperator.EXPLAIN_NAME)) {
          hasEmptyFilter = true;
          break;
        }
        if (row[0].toString().contains(MatchAllFilterOperator.EXPLAIN_NAME)) {
          hasMatchAllFilter = true;
          break;
        }
      }

      if (hasEmptyFilter) {
        if (numRows > maxEmptyFilterDepth) {
          maxEmptyFilterDepth = numRows;
          maxEmptyFilterEntry = entry;
        }
      } else if (hasMatchAllFilter) {
        if (numRows > maxMatchAllFilterDepth) {
          maxMatchAllFilterDepth = numRows;
          maxMatchAllFilterEntry = entry;
        }
      } else {
        if (numRows > maxOtherDepth) {
          maxOtherDepth = numRows;
          maxOtherEntry = entry;
        }
      }
    }

    Map.Entry<ServerRoutingInstance, DataTable> finalEntry;
    if (maxOtherEntry != null) {
      finalEntry = maxOtherEntry;
    } else if (maxMatchAllFilterEntry != null) {
      finalEntry = maxMatchAllFilterEntry;
    } else {
      finalEntry = maxEmptyFilterEntry;
    }

    return finalEntry;
  }

  private void addBrokerReduceOperation(List<Object[]> resultRows) {

    Set<String> postAggregations = new HashSet<>();
    QueryContextUtils.collectPostAggregations(_queryContext, postAggregations);
    StringBuilder stringBuilder = new StringBuilder("BROKER_REDUCE").append('(');

    if (_queryContext.getHavingFilter() != null) {
      stringBuilder.append("havingFilter").append(':').append(_queryContext.getHavingFilter().toString()).append(',');
    }

    if (_queryContext.getOrderByExpressions() != null) {
      stringBuilder.append("sort").append(':').append(_queryContext.getOrderByExpressions().toString()).append(',');
    }

    stringBuilder.append("limit:").append(_queryContext.getLimit());
    if (!postAggregations.isEmpty()) {
      stringBuilder.append(",postAggregations:");
      int count = 0;
      for (String func : postAggregations) {
        if (count == postAggregations.size() - 1) {
          stringBuilder.append(func);
        } else {
          stringBuilder.append(func).append(", ");
        }
        count++;
      }
    }

    String brokerReduceNode = stringBuilder.append(')').toString();
    Object[] brokerReduceRow = new Object[]{brokerReduceNode, 0, -1};

    resultRows.add(brokerReduceRow);
  }
}
