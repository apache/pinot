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

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.roaringbitmap.RoaringBitmap;


/**
 * Helper class to reduce and set Aggregation results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationDataTableReducer implements DataTableReducer {
  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;

  AggregationDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
  }

  /**
   * Reduces data tables and sets aggregations results into ResultTable.
   */
  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    assert dataSchema != null;

    if (dataTableMap.isEmpty()) {
      DataSchema resultTableSchema =
          new PostAggregationHandler(_queryContext, getPrePostAggregationDataSchema()).getResultDataSchema();
      brokerResponseNative.setResultTable(new ResultTable(resultTableSchema, Collections.emptyList()));
      return;
    }

    if (!_queryContext.isServerReturnFinalResult()) {
      reduceWithIntermediateResult(dataSchema, dataTableMap.values(), brokerResponseNative);
    } else {
      Preconditions.checkState(dataTableMap.size() == 1, "Cannot merge final results from multiple servers");
      reduceWithFinalResult(dataSchema, dataTableMap.values().iterator().next(), brokerResponseNative);
    }
  }

  private void reduceWithIntermediateResult(DataSchema dataSchema, Collection<DataTable> dataTables,
      BrokerResponseNative brokerResponseNative) {
    int numAggregationFunctions = _aggregationFunctions.length;
    Object[] intermediateResults = new Object[numAggregationFunctions];
    for (DataTable dataTable : dataTables) {
      for (int i = 0; i < numAggregationFunctions; i++) {
        Object intermediateResultToMerge;
        ColumnDataType columnDataType = dataSchema.getColumnDataType(i);
        if (_queryContext.isNullHandlingEnabled()) {
          RoaringBitmap nullBitmap = dataTable.getNullRowIds(i);
          if (nullBitmap != null && nullBitmap.contains(0)) {
            intermediateResultToMerge = null;
          } else {
            intermediateResultToMerge = AggregationFunctionUtils.getIntermediateResult(dataTable, columnDataType, 0, i);
          }
        } else {
          intermediateResultToMerge = AggregationFunctionUtils.getIntermediateResult(dataTable, columnDataType, 0, i);
        }
        Object mergedIntermediateResult = intermediateResults[i];
        if (mergedIntermediateResult == null) {
          intermediateResults[i] = intermediateResultToMerge;
        } else {
          intermediateResults[i] = _aggregationFunctions[i].merge(mergedIntermediateResult, intermediateResultToMerge);
        }
      }
    }
    Object[] finalResults = new Object[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      Comparable result = aggregationFunction.extractFinalResult(intermediateResults[i]);
      finalResults[i] = result == null ? null : aggregationFunction.getFinalResultColumnType().convert(result);
    }
    brokerResponseNative.setResultTable(reduceToResultTable(finalResults));
  }

  private void reduceWithFinalResult(DataSchema dataSchema, DataTable dataTable,
      BrokerResponseNative brokerResponseNative) {
    int numAggregationFunctions = _aggregationFunctions.length;
    Object[] finalResults = new Object[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      ColumnDataType columnDataType = dataSchema.getColumnDataType(i);
      if (_queryContext.isNullHandlingEnabled()) {
        RoaringBitmap nullBitmap = dataTable.getNullRowIds(i);
        if (nullBitmap != null && nullBitmap.contains(0)) {
          finalResults[i] = null;
        } else {
          finalResults[i] = AggregationFunctionUtils.getConvertedFinalResult(dataTable, columnDataType, 0, i);
        }
      } else {
        finalResults[i] = AggregationFunctionUtils.getConvertedFinalResult(dataTable, columnDataType, 0, i);
      }
    }
    brokerResponseNative.setResultTable(reduceToResultTable(finalResults));
  }

  /**
   * Sets aggregation results into ResultsTable
   */
  private ResultTable reduceToResultTable(Object[] finalResults) {
    PostAggregationHandler postAggregationHandler =
        new PostAggregationHandler(_queryContext, getPrePostAggregationDataSchema());
    DataSchema dataSchema = postAggregationHandler.getResultDataSchema();
    Object[] row = postAggregationHandler.getResult(finalResults);
    ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    for (int i = 0; i < numColumns; i++) {
      row[i] = columnDataTypes[i].format(row[i]);
    }
    return new ResultTable(dataSchema, Collections.singletonList(row));
  }

  /**
   * Constructs the DataSchema for the rows before the post-aggregation (SQL mode).
   */
  private DataSchema getPrePostAggregationDataSchema() {
    int numAggregationFunctions = _aggregationFunctions.length;
    String[] columnNames = new String[numAggregationFunctions];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      columnNames[i] = aggregationFunction.getResultColumnName();
      columnDataTypes[i] = aggregationFunction.getFinalResultColumnType();
    }
    return new DataSchema(columnNames, columnDataTypes);
  }
}
