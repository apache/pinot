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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.RoaringBitmap;


/**
 * Helper class to reduce data tables and set results of distinct query into the BrokerResponseNative
 */
public class DistinctDataTableReducer implements DataTableReducer {
  private final QueryContext _queryContext;

  public DistinctDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
  }

  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    dataSchema = ReducerDataSchemaUtils.canonicalizeDataSchemaForDistinct(_queryContext, dataSchema);
    DistinctTable distinctTable =
        new DistinctTable(dataSchema, _queryContext.getOrderByExpressions(), _queryContext.getLimit(),
            _queryContext.isNullHandlingEnabled());
    if (distinctTable.hasOrderBy()) {
      addToOrderByDistinctTable(dataSchema, dataTableMap, distinctTable);
    } else {
      addToNonOrderByDistinctTable(dataSchema, dataTableMap, distinctTable);
    }
    brokerResponseNative.setResultTable(reduceToResultTable(distinctTable));
  }

  private void addToOrderByDistinctTable(DataSchema dataSchema, Map<ServerRoutingInstance, DataTable> dataTableMap,
      DistinctTable distinctTable) {
    for (DataTable dataTable : dataTableMap.values()) {
      Tracing.ThreadAccountantOps.sampleAndCheckInterruption();
      int numColumns = dataSchema.size();
      int numRows = dataTable.getNumberOfRows();
      if (_queryContext.isNullHandlingEnabled()) {
        RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
        for (int coldId = 0; coldId < numColumns; coldId++) {
          nullBitmaps[coldId] = dataTable.getNullRowIds(coldId);
        }
        for (int rowId = 0; rowId < numRows; rowId++) {
          distinctTable.addWithOrderBy(new Record(
              SelectionOperatorUtils.extractRowFromDataTableWithNullHandling(dataTable, rowId, nullBitmaps)));
        }
      } else {
        for (int rowId = 0; rowId < numRows; rowId++) {
          distinctTable.addWithOrderBy(new Record(SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId)));
        }
      }
    }
  }

  private void addToNonOrderByDistinctTable(DataSchema dataSchema, Map<ServerRoutingInstance, DataTable> dataTableMap,
      DistinctTable distinctTable) {
    for (DataTable dataTable : dataTableMap.values()) {
      Tracing.ThreadAccountantOps.sampleAndCheckInterruption();
      int numColumns = dataSchema.size();
      int numRows = dataTable.getNumberOfRows();
      if (_queryContext.isNullHandlingEnabled()) {
        RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
        for (int coldId = 0; coldId < numColumns; coldId++) {
          nullBitmaps[coldId] = dataTable.getNullRowIds(coldId);
        }
        for (int rowId = 0; rowId < numRows; rowId++) {
          if (distinctTable.addWithoutOrderBy(new Record(
              SelectionOperatorUtils.extractRowFromDataTableWithNullHandling(dataTable, rowId, nullBitmaps)))) {
            return;
          }
        }
      } else {
        for (int rowId = 0; rowId < numRows; rowId++) {
          if (distinctTable.addWithoutOrderBy(
              new Record(SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId)))) {
            return;
          }
        }
      }
    }
  }

  private ResultTable reduceToResultTable(DistinctTable distinctTable) {
    List<Object[]> rows = new ArrayList<>(distinctTable.size());
    DataSchema dataSchema = distinctTable.getDataSchema();
    ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    Iterator<Record> iterator = distinctTable.getFinalResult();
    while (iterator.hasNext()) {
      Object[] values = iterator.next().getValues();
      Object[] row = new Object[numColumns];
      for (int i = 0; i < numColumns; i++) {
        Object value = values[i];
        if (value != null) {
          row[i] = columnDataTypes[i].convertAndFormat(value);
        }
      }
      rows.add(row);
    }
    return new ResultTable(dataSchema, rows);
  }
}
