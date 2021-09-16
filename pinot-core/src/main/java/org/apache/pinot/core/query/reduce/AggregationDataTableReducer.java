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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.QueryOptions;
import org.apache.pinot.spi.utils.DataSchema;
import org.apache.pinot.spi.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.DataTable;


/**
 * Helper class to reduce and set Aggregation results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationDataTableReducer implements DataTableReducer {
  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final boolean _preserveType;
  private final boolean _responseFormatSql;

  AggregationDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    QueryOptions queryOptions = new QueryOptions(queryContext.getQueryOptions());
    _preserveType = queryOptions.isPreserveType();
    _responseFormatSql = queryOptions.isResponseFormatSQL();
  }

  /**
   * Reduces data tables and sets aggregations results into
   * 1. ResultTable if _responseFormatSql is true
   * 2. AggregationResults by default
   */
  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    if (dataTableMap.isEmpty()) {
      if (_responseFormatSql) {
        DataSchema resultTableSchema =
            new PostAggregationHandler(_queryContext, getPrePostAggregationDataSchema()).getResultDataSchema();
        brokerResponseNative.setResultTable(new ResultTable(resultTableSchema, Collections.emptyList()));
      }
      return;
    }

    // Merge results from all data tables
    int numAggregationFunctions = _aggregationFunctions.length;
    Object[] intermediateResults = new Object[numAggregationFunctions];
    for (DataTable dataTable : dataTableMap.values()) {
      for (int i = 0; i < numAggregationFunctions; i++) {
        Object intermediateResultToMerge;
        ColumnDataType columnDataType = dataSchema.getColumnDataType(i);
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
        Object mergedIntermediateResult = intermediateResults[i];
        if (mergedIntermediateResult == null) {
          intermediateResults[i] = intermediateResultToMerge;
        } else {
          intermediateResults[i] = _aggregationFunctions[i].merge(mergedIntermediateResult, intermediateResultToMerge);
        }
      }
    }
    Serializable[] finalResults = new Serializable[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      finalResults[i] = aggregationFunction.getFinalResultColumnType()
          .convert(aggregationFunction.extractFinalResult(intermediateResults[i]));
    }

    if (_responseFormatSql) {
      brokerResponseNative.setResultTable(reduceToResultTable(finalResults));
    } else {
      brokerResponseNative.setAggregationResults(reduceToAggregationResults(finalResults, dataSchema.getColumnNames()));
    }
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
        aggregationResults.add(new AggregationResult(columnNames[i], AggregationFunctionUtils
            .formatValue(_aggregationFunctions[i].getFinalResultColumnType().format(finalResults[i]))));
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
