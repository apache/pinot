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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.transport.ServerRoutingInstance;


/**
 * Helper class to reduce and set Aggregation results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class PreAggregationGapFillDataTableReducer implements DataTableReducer {
  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;

  PreAggregationGapFillDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
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
      DataSchema resultTableSchema =
          new PostAggregationHandler(_queryContext, getPrePostAggregationDataSchema()).getResultDataSchema();
      brokerResponseNative.setResultTable(new ResultTable(resultTableSchema, Collections.emptyList()));
      return;
    }

    PreAggregationGapFillSelectionOperatorService preAggregationGapFillSelectionOperatorService
        = new PreAggregationGapFillSelectionOperatorService(_queryContext, dataSchema);
    List<Object[]> sortedRawRows
        = preAggregationGapFillSelectionOperatorService.reduceWithOrdering(dataTableMap.values());

    String [] columnNames = new String[_queryContext.getSelectExpressions().size()];
    ColumnDataType [] columnDataTypes = new ColumnDataType[_queryContext.getSelectExpressions().size()];
    for (int i = 0; i < _queryContext.getSelectExpressions().size(); i++) {
      ExpressionContext expressionContext = _queryContext.getSelectExpressions().get(i);
      if (expressionContext.getType() != ExpressionContext.Type.FUNCTION) {
        columnNames[i] = expressionContext.getIdentifier();
        columnDataTypes[i] = ColumnDataType.STRING;
      } else {
        FunctionContext functionContext = expressionContext.getFunction();
        AggregationFunction aggregationFunction =
            AggregationFunctionFactory.getAggregationFunction(functionContext, _queryContext);
        columnDataTypes[i] = aggregationFunction.getFinalResultColumnType();
        columnNames[i] = functionContext.toString();
      }
    }
    DataSchema dataSchemaForAggregatedResult = new DataSchema(columnNames, columnDataTypes);
    List<Object[]> aggregateResult = preAggregationGapFillSelectionOperatorService.gapFillAndAggregate(
        sortedRawRows, dataSchemaForAggregatedResult);
    // Merge results from all data tables
    brokerResponseNative.setResultTable(new ResultTable(dataSchemaForAggregatedResult, aggregateResult));
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
