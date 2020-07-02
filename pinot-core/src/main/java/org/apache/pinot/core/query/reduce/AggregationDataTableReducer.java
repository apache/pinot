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
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.QueryOptions;


/**
 * Helper class to reduce and set Aggregation results into the BrokerResponseNative
 */
public class AggregationDataTableReducer implements DataTableReducer {
  private final AggregationFunction[] _aggregationFunctions;
  private final boolean _preserveType;
  private final boolean _responseFormatSql;

  AggregationDataTableReducer(BrokerRequest brokerRequest, AggregationFunction[] aggregationFunctions,
      QueryOptions queryOptions) {
    _aggregationFunctions = aggregationFunctions;
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
      BrokerMetrics brokerMetrics) {
    if (dataTableMap.isEmpty()) {
      if (_responseFormatSql) {
        DataSchema finalDataSchema = getResultTableDataSchema();
        brokerResponseNative.setResultTable(new ResultTable(finalDataSchema, Collections.emptyList()));
      }
      return;
    }

    // Merge results from all data tables
    int numAggregationFunctions = _aggregationFunctions.length;
    Object[] intermediateResults = new Object[numAggregationFunctions];
    for (DataTable dataTable : dataTableMap.values()) {
      for (int i = 0; i < numAggregationFunctions; i++) {
        Object intermediateResultToMerge;
        DataSchema.ColumnDataType columnDataType = dataSchema.getColumnDataType(i);
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

    if (_responseFormatSql) {
      brokerResponseNative.setResultTable(reduceToResultTable(intermediateResults));
    } else {
      brokerResponseNative.setAggregationResults(reduceToAggregationResult(intermediateResults, dataSchema));
    }
  }

  /**
   * Sets aggregation results into ResultsTable
   */
  private ResultTable reduceToResultTable(Object[] intermediateResults) {
    List<Object[]> rows = new ArrayList<>(1);
    int numAggregationFunctions = _aggregationFunctions.length;
    Object[] row = new Object[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      row[i] = _aggregationFunctions[i].extractFinalResult(intermediateResults[i]);
    }
    rows.add(row);

    DataSchema finalDataSchema = getResultTableDataSchema();
    return new ResultTable(finalDataSchema, rows);
  }

  /**
   * Sets aggregation results into AggregationResults
   */
  private List<AggregationResult> reduceToAggregationResult(Object[] intermediateResults, DataSchema dataSchema) {
    // Extract final results and set them into the broker response
    int numAggregationFunctions = _aggregationFunctions.length;
    List<AggregationResult> reducedAggregationResults = new ArrayList<>(numAggregationFunctions);
    for (int i = 0; i < numAggregationFunctions; i++) {
      Serializable resultValue = AggregationFunctionUtils
          .getSerializableValue(_aggregationFunctions[i].extractFinalResult(intermediateResults[i]));

      // Format the value into string if required
      if (!_preserveType) {
        resultValue = AggregationFunctionUtils.formatValue(resultValue);
      }
      reducedAggregationResults.add(new AggregationResult(dataSchema.getColumnName(i), resultValue));
    }
    return reducedAggregationResults;
  }

  /**
   * Constructs the data schema for the final results table
   */
  private DataSchema getResultTableDataSchema() {
    int numAggregationFunctions = _aggregationFunctions.length;
    String[] finalColumnNames = new String[numAggregationFunctions];
    DataSchema.ColumnDataType[] finalColumnDataTypes = new DataSchema.ColumnDataType[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      finalColumnNames[i] = aggregationFunction.getResultColumnName();
      finalColumnDataTypes[i] = aggregationFunction.getFinalResultColumnType();
    }
    return new DataSchema(finalColumnNames, finalColumnDataTypes);
  }
}
