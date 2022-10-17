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
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.roaringbitmap.RoaringBitmap;


/**
 * Helper class to reduce data tables and set results of distinct query into the BrokerResponseNative
 */
public class DistinctDataTableReducer implements DataTableReducer {
  private final DistinctAggregationFunction _distinctAggregationFunction;
  private final QueryContext _queryContext;

  DistinctDataTableReducer(DistinctAggregationFunction distinctAggregationFunction, QueryContext queryContext) {
    _distinctAggregationFunction = distinctAggregationFunction;
    _queryContext = queryContext;
  }

  /**
   * Reduces and sets results of distinct into ResultTable.
   */
  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    // DISTINCT is implemented as an aggregation function in the execution engine. Just like
    // other aggregation functions, DISTINCT returns its result as a single object
    // (of type DistinctTable) serialized by the server into the DataTable and deserialized
    // by the broker from the DataTable. So there should be exactly 1 row and 1 column and that
    // column value should be the serialized DistinctTable -- so essentially it is a DataTable
    // inside a DataTable

    // Gather all non-empty DistinctTables
    // TODO: until we upgrade to newer version of pinot, we have to keep both code path. remove after 0.12.0 release.
    // This is to work with server rolling upgrade when partially returned as DistinctTable Obj and partially regular
    // DataTable; if all returns are DataTable we can directly merge with priority queue (with dedup).
    List<DistinctTable> nonEmptyDistinctTables = new ArrayList<>(dataTableMap.size());
    for (DataTable dataTable : dataTableMap.values()) {
      // Do not use the cached data schema because it might be either single object (legacy) or normal data table
      dataSchema = dataTable.getDataSchema();
      int numColumns = dataSchema.size();
      if (numColumns == 1 && dataSchema.getColumnDataType(0) == ColumnDataType.OBJECT) {
        // DistinctTable is still being returned as a single object
        DataTable.CustomObject customObject = dataTable.getCustomObject(0, 0);
        assert customObject != null;
        DistinctTable distinctTable = ObjectSerDeUtils.deserialize(customObject);
        if (!distinctTable.isEmpty()) {
          nonEmptyDistinctTables.add(distinctTable);
        }
      } else {
        // DistinctTable is being returned as normal data table
        int numRows = dataTable.getNumberOfRows();
        if (numRows > 0) {
          List<Record> records = new ArrayList<>(numRows);
          if (_queryContext.isNullHandlingEnabled()) {
            RoaringBitmap[] nullBitmaps = new RoaringBitmap[numColumns];
            for (int coldId = 0; coldId < numColumns; coldId++) {
              nullBitmaps[coldId] = dataTable.getNullRowIds(coldId);
            }
            for (int rowId = 0; rowId < numRows; rowId++) {
              records.add(new Record(
                  SelectionOperatorUtils.extractRowFromDataTableWithNullHandling(dataTable, rowId, nullBitmaps)));
            }
          } else {
            for (int rowId = 0; rowId < numRows; rowId++) {
              records.add(new Record(SelectionOperatorUtils.extractRowFromDataTable(dataTable, rowId)));
            }
          }
          nonEmptyDistinctTables.add(new DistinctTable(dataSchema, records));
        }
      }
    }

    if (nonEmptyDistinctTables.isEmpty()) {
      // All the DistinctTables are empty, construct an empty response
      // TODO: This returns schema with all STRING data types.
      //       There's no way currently to get the data types of the distinct columns for empty results
      String[] columns = _distinctAggregationFunction.getColumns();

      int numColumns = columns.length;
      ColumnDataType[] columnDataTypes = new ColumnDataType[numColumns];
      Arrays.fill(columnDataTypes, ColumnDataType.STRING);
      brokerResponseNative.setResultTable(
          new ResultTable(new DataSchema(columns, columnDataTypes), Collections.emptyList()));
    } else {
      // Construct a main DistinctTable and merge all non-empty DistinctTables into it
      DistinctTable mainDistinctTable = new DistinctTable(nonEmptyDistinctTables.get(0).getDataSchema(),
          _distinctAggregationFunction.getOrderByExpressions(), _distinctAggregationFunction.getLimit(),
          _queryContext.isNullHandlingEnabled());
      for (DistinctTable distinctTable : nonEmptyDistinctTables) {
        mainDistinctTable.mergeTable(distinctTable);
      }
      brokerResponseNative.setResultTable(reduceToResultTable(mainDistinctTable));
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
