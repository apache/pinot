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
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;


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
    List<DistinctTable> nonEmptyDistinctTables = new ArrayList<>(dataTableMap.size());
    for (DataTable dataTable : dataTableMap.values()) {
      DistinctTable distinctTable = dataTable.getObject(0, 0);
      if (!distinctTable.isEmpty()) {
        nonEmptyDistinctTables.add(distinctTable);
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
