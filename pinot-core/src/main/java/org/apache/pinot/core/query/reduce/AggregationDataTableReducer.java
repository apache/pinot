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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
import org.apache.pinot.core.query.utils.rewriter.ResultRewriteUtils;
import org.apache.pinot.core.query.utils.rewriter.RewriterResult;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.RoaringBitmap;


/**
 * Helper class to reduce and set Aggregation results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationDataTableReducer implements DataTableReducer {
  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;

  public AggregationDataTableReducer(QueryContext queryContext) {
    _queryContext = queryContext;
    _aggregationFunctions = _queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
  }

  /**
   * Reduces data tables and sets aggregations results into ResultTable.
   */
  @Override
  public void reduceAndSetResults(String tableName, DataSchema dataSchema,
      Map<ServerRoutingInstance, DataTable> dataTableMap, BrokerResponseNative brokerResponseNative,
      DataTableReducerContext reducerContext, BrokerMetrics brokerMetrics) {
    dataSchema = ReducerDataSchemaUtils.canonicalizeDataSchemaForAggregation(_queryContext, dataSchema);

    if (dataTableMap.isEmpty()) {
      DataSchema resultTableSchema =
          new PostAggregationHandler(_queryContext, getPrePostAggregationDataSchema(dataSchema)).getResultDataSchema();
      brokerResponseNative.setResultTable(new ResultTable(resultTableSchema, Collections.emptyList()));
      return;
    }

    Collection<DataTable> dataTables = dataTableMap.values();
    if (_queryContext.isServerReturnFinalResult()) {
      if (dataTables.size() == 1) {
        processSingleFinalResult(dataSchema, dataTables.iterator().next(), brokerResponseNative);
      } else {
        reduceWithFinalResult(dataSchema, dataTables, brokerResponseNative);
      }
    } else {
      reduceWithIntermediateResult(dataSchema, dataTables, brokerResponseNative);
    }
  }

  private void reduceWithIntermediateResult(DataSchema dataSchema, Collection<DataTable> dataTables,
      BrokerResponseNative brokerResponseNative) {
    int numAggregationFunctions = _aggregationFunctions.length;
    Object[] intermediateResults = new Object[numAggregationFunctions];
    for (DataTable dataTable : dataTables) {
      Tracing.ThreadAccountantOps.sampleAndCheckInterruption();
      for (int i = 0; i < numAggregationFunctions; i++) {
        AggregationFunction aggregationFunction = _aggregationFunctions[i];
        Object intermediateResultToMerge;
        ColumnDataType columnDataType = dataSchema.getColumnDataType(i);
        if (_queryContext.isNullHandlingEnabled()) {
          RoaringBitmap nullBitmap = dataTable.getNullRowIds(i);
          if (nullBitmap != null && nullBitmap.contains(0)) {
            intermediateResultToMerge = null;
          } else {
            intermediateResultToMerge =
                AggregationFunctionUtils.getIntermediateResult(aggregationFunction, dataTable, columnDataType, 0, i);
          }
        } else {
          intermediateResultToMerge =
              AggregationFunctionUtils.getIntermediateResult(aggregationFunction, dataTable, columnDataType, 0, i);
        }
        Object mergedIntermediateResult = intermediateResults[i];
        if (mergedIntermediateResult == null) {
          intermediateResults[i] = intermediateResultToMerge;
        } else {
          intermediateResults[i] = aggregationFunction.merge(mergedIntermediateResult, intermediateResultToMerge);
        }
      }
    }
    Object[] finalResults = new Object[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      Comparable result = aggregationFunction.extractFinalResult(intermediateResults[i]);
      finalResults[i] = result != null ? aggregationFunction.getFinalResultColumnType().convert(result) : null;
    }
    brokerResponseNative.setResultTable(reduceToResultTable(getPrePostAggregationDataSchema(dataSchema), finalResults));
  }

  private void processSingleFinalResult(DataSchema dataSchema, DataTable dataTable,
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
    brokerResponseNative.setResultTable(reduceToResultTable(dataSchema, finalResults));
  }

  private void reduceWithFinalResult(DataSchema dataSchema, Collection<DataTable> dataTables,
      BrokerResponseNative brokerResponseNative) {
    int numAggregationFunctions = _aggregationFunctions.length;
    Comparable[] finalResults = new Comparable[numAggregationFunctions];
    for (DataTable dataTable : dataTables) {
      for (int i = 0; i < numAggregationFunctions; i++) {
        Tracing.ThreadAccountantOps.sampleAndCheckInterruption();
        Comparable finalResultToMerge;
        ColumnDataType columnDataType = dataSchema.getColumnDataType(i);
        if (_queryContext.isNullHandlingEnabled()) {
          RoaringBitmap nullBitmap = dataTable.getNullRowIds(i);
          if (nullBitmap != null && nullBitmap.contains(0)) {
            finalResultToMerge = null;
          } else {
            finalResultToMerge = AggregationFunctionUtils.getFinalResult(dataTable, columnDataType, 0, i);
          }
        } else {
          finalResultToMerge = AggregationFunctionUtils.getFinalResult(dataTable, columnDataType, 0, i);
        }
        Comparable mergedFinalResult = finalResults[i];
        if (mergedFinalResult == null) {
          finalResults[i] = finalResultToMerge;
        } else {
          finalResults[i] = _aggregationFunctions[i].mergeFinalResult(mergedFinalResult, finalResultToMerge);
        }
      }
    }
    Object[] convertedFinalResults = new Object[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      Comparable result = finalResults[i];
      convertedFinalResults[i] = result != null ? aggregationFunction.getFinalResultColumnType().convert(result) : null;
    }
    brokerResponseNative.setResultTable(
        reduceToResultTable(getPrePostAggregationDataSchema(dataSchema), convertedFinalResults));
  }

  /**
   * Sets aggregation results into ResultsTable
   */
  private ResultTable reduceToResultTable(DataSchema dataSchema, Object[] finalResults) {
    PostAggregationHandler postAggregationHandler = new PostAggregationHandler(_queryContext, dataSchema);
    DataSchema resultDataSchema = postAggregationHandler.getResultDataSchema();
    Object[] row = postAggregationHandler.getResult(finalResults);

    RewriterResult resultRewriterResult =
        ResultRewriteUtils.rewriteResult(resultDataSchema, Collections.singletonList(row));
    resultDataSchema = resultRewriterResult.getDataSchema();
    List<Object[]> rows = resultRewriterResult.getRows();

    ColumnDataType[] columnDataTypes = resultDataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    for (Object[] rewrittenRow : rows) {
      for (int j = 0; j < numColumns; j++) {
        rewrittenRow[j] = columnDataTypes[j].format(rewrittenRow[j]);
      }
    }

    return new ResultTable(resultDataSchema, rows);
  }

  /**
   * Constructs the DataSchema for the rows before the post-aggregation (SQL mode).
   */
  private DataSchema getPrePostAggregationDataSchema(DataSchema dataSchema) {
    int numAggregationFunctions = _aggregationFunctions.length;
    ColumnDataType[] columnDataTypes = new ColumnDataType[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      columnDataTypes[i] = _aggregationFunctions[i].getFinalResultColumnType();
    }
    return new DataSchema(dataSchema.getColumnNames(), columnDataTypes);
  }
}
