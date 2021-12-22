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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * Helper class to reduce data tables and set group by results into the BrokerResponseNative
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class BaseGroupByReducer {
  protected final QueryContext _queryContext;
  protected final AggregationFunction[] _aggregationFunctions;
  protected final int _numAggregationFunctions;
  protected final List<ExpressionContext> _groupByExpressions;
  protected final int _numGroupByExpressions;
  protected final int _numColumns;

  public BaseGroupByReducer(QueryContext queryContext) {
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    _numAggregationFunctions = _aggregationFunctions.length;
    _groupByExpressions = queryContext.getGroupByExpressions();
    assert _groupByExpressions != null;
    _numGroupByExpressions = _groupByExpressions.size();
    _numColumns = _numAggregationFunctions + _numGroupByExpressions;
  }

  /**
   * Creates a result table of the group by results from the indexed table.
   */
  protected ResultTable getResultTable(IndexedTable indexedTable) {
    DataSchema dataSchema = indexedTable.getDataSchema();
    Iterator<Record> sortedIterator = indexedTable.iterator();
    DataSchema prePostAggregationDataSchema = getPrePostAggregationDataSchema(dataSchema);
    ColumnDataType[] columnDataTypes = prePostAggregationDataSchema.getColumnDataTypes();
    int numColumns = columnDataTypes.length;
    int limit = _queryContext.getLimit();
    List<Object[]> rows = new ArrayList<>(limit);

    PostAggregationHandler postAggregationHandler =
        new PostAggregationHandler(_queryContext, prePostAggregationDataSchema);
    FilterContext havingFilter = _queryContext.getHavingFilter();
    if (havingFilter != null) {
      HavingFilterHandler havingFilterHandler = new HavingFilterHandler(havingFilter, postAggregationHandler);
      while (rows.size() < limit && sortedIterator.hasNext()) {
        Object[] row = sortedIterator.next().getValues();
        extractFinalAggregationResults(row);
        for (int i = 0; i < numColumns; i++) {
          row[i] = columnDataTypes[i].convert(row[i]);
        }
        if (havingFilterHandler.isMatch(row)) {
          rows.add(row);
        }
      }
    } else {
      for (int i = 0; i < limit && sortedIterator.hasNext(); i++) {
        Object[] row = sortedIterator.next().getValues();
        extractFinalAggregationResults(row);
        for (int j = 0; j < numColumns; j++) {
          row[j] = columnDataTypes[j].convert(row[j]);
        }
        rows.add(row);
      }
    }
    DataSchema resultDataSchema = postAggregationHandler.getResultDataSchema();
    ColumnDataType[] resultColumnDataTypes = resultDataSchema.getColumnDataTypes();
    int numResultColumns = resultColumnDataTypes.length;
    int numResultRows = rows.size();
    List<Object[]> resultRows = new ArrayList<>(numResultRows);
    for (Object[] row : rows) {
      Object[] resultRow = postAggregationHandler.getResult(row);
      for (int i = 0; i < numResultColumns; i++) {
        resultRow[i] = resultColumnDataTypes[i].format(resultRow[i]);
      }
      resultRows.add(resultRow);
    }
    return new ResultTable(resultDataSchema, resultRows);
  }

  /**
   * Constructs the DataSchema for the rows before the post-aggregation (SQL mode).
   */
  protected DataSchema getPrePostAggregationDataSchema(DataSchema dataSchema) {
    String[] columnNames = dataSchema.getColumnNames();
    ColumnDataType[] columnDataTypes = new ColumnDataType[_numColumns];
    System.arraycopy(dataSchema.getColumnDataTypes(), 0, columnDataTypes, 0, _numGroupByExpressions);
    for (int i = 0; i < _numAggregationFunctions; i++) {
      columnDataTypes[i + _numGroupByExpressions] = _aggregationFunctions[i].getFinalResultColumnType();
    }
    return new DataSchema(columnNames, columnDataTypes);
  }

  /**
   * Extracts the final aggregation results for the given row (in-place).
   */
  protected void extractFinalAggregationResults(Object[] row) {
    for (int i = 0; i < _numAggregationFunctions; i++) {
      int valueIndex = i + _numGroupByExpressions;
      row[valueIndex] = _aggregationFunctions[i].extractFinalResult(row[valueIndex]);
    }
  }
}
