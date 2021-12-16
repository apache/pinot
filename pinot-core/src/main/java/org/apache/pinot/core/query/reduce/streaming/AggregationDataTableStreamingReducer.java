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
package org.apache.pinot.core.query.reduce.streaming;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.reduce.DataTableReducerContext;
import org.apache.pinot.core.query.reduce.PostAggregationHandler;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.QueryOptionsUtils;


/**
 * Helper class to reduce and set Aggregation results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationDataTableStreamingReducer implements StreamingReducer {
  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final boolean _preserveType;
  private final int _numAggregationFunctions;
  private final Object[] _intermediateResults;

  private DataSchema _dataSchema;
  private DataTableReducerContext _dataTableReducerContext;

  public AggregationDataTableStreamingReducer(QueryContext queryContext) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    Map<String, String> queryOptions = queryContext.getQueryOptions();
    _preserveType = QueryOptionsUtils.isPreserveType(queryOptions);

    Preconditions.checkNotNull(_aggregationFunctions);
    _numAggregationFunctions = _aggregationFunctions.length;
    _intermediateResults = new Object[_numAggregationFunctions];
    _dataSchema = null;
  }

  @Override
  public void init(DataTableReducerContext dataTableReducerContext) {
    _dataTableReducerContext = dataTableReducerContext;
  }

  /**
   * Reduces data tables and sets aggregations results into
   * 1. ResultTable if _responseFormatSql is true
   * 2. AggregationResults by default
   */
  @Override
  public synchronized void reduce(ServerRoutingInstance key, DataTable dataTable) {
    _dataSchema = _dataSchema == null ? dataTable.getDataSchema() : _dataSchema;
    // Merge results from all data tables

    for (int i = 0; i < _numAggregationFunctions; i++) {
      Object intermediateResultToMerge;
      ColumnDataType columnDataType = _dataSchema.getColumnDataType(i);
      switch (columnDataType) {
        case LONG:
          intermediateResultToMerge = dataTable.getLong(0, i);
          break;
        case DOUBLE:
          intermediateResultToMerge = dataTable.getDouble(0, i);
          break;
        case OBJECT:
          intermediateResultToMerge = dataTable.getObject(0, i);
          break;
        default:
          throw new IllegalStateException("Illegal column data type in aggregation results: " + columnDataType);
      }
      Object mergedIntermediateResult = _intermediateResults[i];
      if (mergedIntermediateResult == null) {
        _intermediateResults[i] = intermediateResultToMerge;
      } else {
        _intermediateResults[i] = _aggregationFunctions[i].merge(mergedIntermediateResult, intermediateResultToMerge);
      }
    }
  }

  @Override
  public BrokerResponseNative seal() {

    Serializable[] finalResults = new Serializable[_numAggregationFunctions];
    for (int i = 0; i < _numAggregationFunctions; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      finalResults[i] = aggregationFunction.getFinalResultColumnType()
          .convert(aggregationFunction.extractFinalResult(_intermediateResults[i]));
    }

    BrokerResponseNative brokerResponseNative = new BrokerResponseNative();
    brokerResponseNative.setResultTable(reduceToResultTable(finalResults));
    return brokerResponseNative;
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
   * Sets aggregation results into AggregationResults
   */
  private List<AggregationResult> reduceToAggregationResults(Serializable[] finalResults, String[] columnNames) {
    int numAggregationFunctions = _aggregationFunctions.length;
    List<AggregationResult> aggregationResults = new ArrayList<>(numAggregationFunctions);
    if (_preserveType) {
      for (int i = 0; i < numAggregationFunctions; i++) {
        aggregationResults.add(new AggregationResult(columnNames[i],
            _aggregationFunctions[i].getFinalResultColumnType().format(finalResults[i])));
      }
    } else {
      // Format the values into strings
      for (int i = 0; i < numAggregationFunctions; i++) {
        aggregationResults.add(new AggregationResult(columnNames[i], AggregationFunctionUtils.formatValue(
            _aggregationFunctions[i].getFinalResultColumnType().format(finalResults[i]))));
      }
    }
    return aggregationResults;
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
