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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.selection.SelectionOperatorService;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SelectionResultSetter extends ResultSetter {

  private static final Logger LOGGER = LoggerFactory.getLogger(SelectionResultSetter.class);

  public SelectionResultSetter(String tableName, BrokerRequest brokerRequest, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      BrokerMetrics brokerMetrics) {
    super(tableName, brokerRequest, dataSchema, dataTableMap, brokerResponseNative, brokerMetrics);
  }

  public void setSelectionResults() {
    Selection selection = _brokerRequest.getSelections();

    if (_dataTableMap.isEmpty()) {
      // For empty data table map, construct empty result using the cached data schema for selection query if exists
      if (_dataSchema != null) {
        if (_responseFormatSql) {
          _brokerResponseNative.setResultTable(new ResultTable(_dataSchema, new ArrayList<>(0)));
        } else {
          List<String> selectionColumns =
              SelectionOperatorUtils.getSelectionColumns(selection.getSelectionColumns(), _dataSchema);
          _brokerResponseNative.setSelectionResults(new SelectionResults(selectionColumns, new ArrayList<>(0)));
        }
      }
    } else {

      assert _dataSchema != null;

      // For data table map with more than one data tables, remove conflicting data tables
      if (_dataTableMap.size() > 1) {
        List<ServerRoutingInstance> droppedServers = removeConflictingResponses();
        if (!droppedServers.isEmpty()) {
          String errorMessage =
              QueryException.MERGE_RESPONSE_ERROR.getMessage() + ": responses for table: " + _tableName
                  + " from servers: " + droppedServers + " got dropped due to data schema inconsistency.";
          LOGGER.warn(errorMessage);
          if (_brokerMetrics != null) {
            _brokerMetrics.addMeteredTableValue(TableNameBuilder.extractRawTableName(_tableName),
                BrokerMeter.RESPONSE_MERGE_EXCEPTIONS, 1L);
          }
          _brokerResponseNative
              .addToExceptions(new QueryProcessingException(QueryException.MERGE_RESPONSE_ERROR_CODE, errorMessage));
        }
      }

      int selectionSize = selection.getSize();
      if (selectionSize > 0 && selection.isSetSelectionSortSequence()) {
        // Selection order-by
        SelectionOperatorService selectionService = new SelectionOperatorService(selection, _dataSchema);
        selectionService.reduceWithOrdering(_dataTables);
        if (_responseFormatSql) {
          _brokerResponseNative.setResultTable(selectionService.renderResultTableWithOrdering(_preserveType));
        } else {
          _brokerResponseNative.setSelectionResults(selectionService.renderSelectionResultsWithOrdering(_preserveType));
        }
      } else {
        // Selection only
        List<String> selectionColumns =
            SelectionOperatorUtils.getSelectionColumns(selection.getSelectionColumns(), _dataSchema);
        List<Serializable[]> reducedRows =
            SelectionOperatorUtils.reduceWithoutOrdering(_dataTables, selectionSize);
        if (_responseFormatSql) {
          _brokerResponseNative.setResultTable(
              SelectionOperatorUtils.renderResultTableWithoutOrdering(reducedRows, _dataSchema, _preserveType));
        } else {
          _brokerResponseNative.setSelectionResults(SelectionOperatorUtils
              .renderSelectionResultsWithoutOrdering(reducedRows, _dataSchema, selectionColumns, _preserveType));
        }
      }
    }
  }

  /**
   * Given a data schema, remove data tables that are not compatible with this data schema.
   * <p>Upgrade the data schema passed in to cover all remaining data schemas.
   *
   * @return list of server names where the data table got removed.
   */
  private List<ServerRoutingInstance> removeConflictingResponses() {
    List<ServerRoutingInstance> droppedServers = new ArrayList<>();
    Iterator<Map.Entry<ServerRoutingInstance, DataTable>> iterator = _dataTableMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<ServerRoutingInstance, DataTable> entry = iterator.next();
      DataSchema dataSchemaToCompare = entry.getValue().getDataSchema();
      assert dataSchemaToCompare != null;
      if (!_dataSchema.isTypeCompatibleWith(dataSchemaToCompare)) {
        droppedServers.add(entry.getKey());
        iterator.remove();
      } else {
        _dataSchema.upgradeToCover(dataSchemaToCompare);
      }
    }
    return droppedServers;
  }
}
