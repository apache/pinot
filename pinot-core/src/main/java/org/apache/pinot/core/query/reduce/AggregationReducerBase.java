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
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Helper class to reduce and set Aggregation results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class AggregationReducerBase {
  protected final QueryContext _queryContext;
  protected final AggregationFunction[] _aggregationFunctions;
  protected boolean _preserveType;
  protected boolean _responseFormatSql;

  public AggregationReducerBase(QueryContext queryContext, AggregationFunction[] aggregationFunctions) {
    _queryContext = queryContext;
    _aggregationFunctions = aggregationFunctions;
  }

  protected void mergedResults(Object[] intermediateResults, DataSchema dataSchema, DataTable dataTable) {
    for (int i = 0; i < _aggregationFunctions.length; i++) {
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

  /**
   * Sets aggregation results into ResultsTable
   */
  protected ResultTable reduceToResultTable(Object[] finalResults) {
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
  protected List<AggregationResult> reduceToAggregationResults(Serializable[] finalResults, String[] columnNames) {
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
  protected DataSchema getPrePostAggregationDataSchema() {
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
