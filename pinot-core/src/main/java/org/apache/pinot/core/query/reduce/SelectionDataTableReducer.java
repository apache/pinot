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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorService;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to reduce and set Selection results into the BrokerResponseNative
 */
public class SelectionDataTableReducer implements DataTableReducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(SelectionDataTableReducer.class);

  private final QueryContext _queryContext;

  SelectionDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
  }

  /**
   * Reduces data tables and sets selection results into ResultTable.
   */
  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    if (dataTableMap.isEmpty()) {
      // For empty data table map, construct empty result using the cached data schema for selection query
      DataSchema selectionDataSchema = SelectionOperatorUtils.getResultTableDataSchema(_queryContext, dataSchema);
      brokerResponseNative.setResultTable(new ResultTable(selectionDataSchema, Collections.emptyList()));
      return;
    }

    // For data table map with more than one data tables, remove conflicting data tables
    if (dataTableMap.size() > 1) {
      DataSchema.ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
      List<ServerRoutingInstance> droppedServers = new ArrayList<>();
      Iterator<Map.Entry<ServerRoutingInstance, DataTable>> iterator = dataTableMap.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<ServerRoutingInstance, DataTable> entry = iterator.next();
        DataSchema dataSchemaToCompare = entry.getValue().getDataSchema();
        assert dataSchemaToCompare != null;
        if (!Arrays.equals(columnDataTypes, dataSchemaToCompare.getColumnDataTypes())) {
          droppedServers.add(entry.getKey());
          iterator.remove();
        }
      }
      if (!droppedServers.isEmpty()) {
        String errorMessage =
            QueryException.MERGE_RESPONSE_ERROR.getMessage() + ": responses for table: " + tableName + " from servers: "
                + droppedServers + " got dropped due to data schema inconsistency.";
        LOGGER.warn(errorMessage);
        if (brokerMetrics != null) {
          brokerMetrics.addMeteredTableValue(TableNameBuilder.extractRawTableName(tableName),
              BrokerMeter.RESPONSE_MERGE_EXCEPTIONS, 1L);
        }
        brokerResponseNative.addToExceptions(
            new QueryProcessingException(QueryException.MERGE_RESPONSE_ERROR_CODE, errorMessage));
      }
    }

    int limit = _queryContext.getLimit();
    if (limit > 0 && _queryContext.getOrderByExpressions() != null) {
      // Selection order-by
      SelectionOperatorService selectionService = new SelectionOperatorService(_queryContext, dataSchema);
      selectionService.reduceWithOrdering(dataTableMap.values(), _queryContext.isNullHandlingEnabled());
      brokerResponseNative.setResultTable(selectionService.renderResultTableWithOrdering());
    } else {
      // Selection only
      List<String> selectionColumns = SelectionOperatorUtils.getSelectionColumns(_queryContext, dataSchema);
      List<Object[]> reducedRows = SelectionOperatorUtils.reduceWithoutOrdering(dataTableMap.values(), limit,
          _queryContext.isNullHandlingEnabled());
      brokerResponseNative.setResultTable(
          SelectionOperatorUtils.renderResultTableWithoutOrdering(reducedRows, dataSchema, selectionColumns));
    }
  }
}
