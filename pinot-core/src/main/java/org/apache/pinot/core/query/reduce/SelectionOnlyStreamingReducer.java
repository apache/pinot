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
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.roaringbitmap.RoaringBitmap;


public class SelectionOnlyStreamingReducer implements StreamingReducer {
  private final QueryContext _queryContext;

  private DataSchema _dataSchema;
  private List<Object[]> _rows;

  public SelectionOnlyStreamingReducer(QueryContext queryContext) {
    _queryContext = queryContext;
  }

  @Override
  public void init(DataTableReducerContext dataTableReducerContext) {
    _rows = new ArrayList<>(Math.min(_queryContext.getLimit(), SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY));
  }

  @Override
  public synchronized void reduce(ServerRoutingInstance key, DataTable dataTable) {
    // get dataSchema
    _dataSchema = _dataSchema == null ? dataTable.getDataSchema() : _dataSchema;
    // TODO: For data table map with more than one data tables, remove conflicting data tables
    reduceWithoutOrdering(dataTable, _queryContext.getLimit(), _queryContext.isNullHandlingEnabled());
  }

  private void reduceWithoutOrdering(DataTable dataTable, int limit, boolean nullHandlingEnabled) {
    int numColumns = dataTable.getDataSchema().size();
    int numRows = dataTable.getNumberOfRows();
    if (nullHandlingEnabled) {
      RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
      for (int coldId = 0; coldId < numColumns; coldId++) {
        nullBitmaps[coldId] = dataTable.getNullRowIds(coldId);
      }
      for (int rowId = 0; rowId < numRows; rowId++) {
        if (_rows.size() < limit) {
          Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId);
          for (int colId = 0; colId < numColumns; colId++) {
            if (nullBitmaps[colId] != null && nullBitmaps[colId].contains(rowId)) {
              row[colId] = null;
            }
          }
          _rows.add(row);
        } else {
          break;
        }
      }
    } else {
      for (int rowId = 0; rowId < numRows; rowId++) {
        if (_rows.size() < limit) {
          _rows.add(SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId));
        } else {
          break;
        }
      }
    }
  }

  @Override
  public BrokerResponseNative seal() {
    if (_dataSchema == null) {
      return BrokerResponseNative.empty();
    }
    Pair<DataSchema, int[]> pair =
        SelectionOperatorUtils.getResultTableDataSchemaAndColumnIndices(_queryContext, _dataSchema);
    ResultTable resultTable;
    if (_rows.isEmpty()) {
      resultTable = new ResultTable(pair.getLeft(), Collections.emptyList());
    } else {
      resultTable = SelectionOperatorUtils.renderResultTableWithoutOrdering(_rows, pair.getLeft(), pair.getRight());
    }
    BrokerResponseNative brokerResponse = new BrokerResponseNative();
    brokerResponse.setResultTable(resultTable);
    return brokerResponse;
  }
}
