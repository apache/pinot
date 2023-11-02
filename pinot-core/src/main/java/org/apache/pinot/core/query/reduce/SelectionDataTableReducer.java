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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorService;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;


/**
 * Helper class to reduce and set Selection results into the BrokerResponseNative
 */
public class SelectionDataTableReducer implements DataTableReducer {
  private final QueryContext _queryContext;

  public SelectionDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
  }

  /**
   * Reduces data tables and sets selection results into ResultTable.
   */
  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    Pair<DataSchema, int[]> pair =
        SelectionOperatorUtils.getResultTableDataSchemaAndColumnIndices(_queryContext, dataSchema);
    int limit = _queryContext.getLimit();
    if (dataTableMap.isEmpty() || limit == 0) {
      brokerResponseNative.setResultTable(new ResultTable(pair.getLeft(), Collections.emptyList()));
      return;
    }
    if (_queryContext.getOrderByExpressions() == null) {
      // Selection only
      List<Object[]> reducedRows = SelectionOperatorUtils.reduceWithoutOrdering(dataTableMap.values(), limit,
          _queryContext.getNullMode());
      brokerResponseNative.setResultTable(
          SelectionOperatorUtils.renderResultTableWithoutOrdering(reducedRows, pair.getLeft(), pair.getRight()));
    } else {
      // Selection order-by
      SelectionOperatorService selectionService =
          new SelectionOperatorService(_queryContext, pair.getLeft(), pair.getRight());
      selectionService.reduceWithOrdering(dataTableMap.values());
      brokerResponseNative.setResultTable(selectionService.renderResultTableWithOrdering());
    }
  }
}
