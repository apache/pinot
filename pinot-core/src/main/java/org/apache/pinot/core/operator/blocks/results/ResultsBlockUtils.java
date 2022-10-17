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
package org.apache.pinot.core.operator.blocks.results;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctAggregationFunction;
import org.apache.pinot.core.query.distinct.DistinctTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;


@SuppressWarnings("rawtypes")
public class ResultsBlockUtils {
  private ResultsBlockUtils() {
  }

  public static BaseResultsBlock buildEmptyQueryResults(QueryContext queryContext) {
    if (QueryContextUtils.isSelectionQuery(queryContext)) {
      return buildEmptySelectionQueryResults(queryContext);
    } else if (QueryContextUtils.isAggregationQuery(queryContext)) {
      if (queryContext.getGroupByExpressions() == null) {
        return buildEmptyAggregationQueryResults(queryContext);
      } else {
        return buildEmptyGroupByQueryResults(queryContext);
      }
    } else {
      assert QueryContextUtils.isDistinctQuery(queryContext);
      return buildEmptyDistinctQueryResults(queryContext);
    }
  }

  private static SelectionResultsBlock buildEmptySelectionQueryResults(QueryContext queryContext) {
    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    int numSelectExpressions = selectExpressions.size();
    String[] columnNames = new String[numSelectExpressions];
    for (int i = 0; i < numSelectExpressions; i++) {
      columnNames[i] = selectExpressions.get(i).toString();
    }
    ColumnDataType[] columnDataTypes = new ColumnDataType[numSelectExpressions];
    // NOTE: Use STRING column data type as default for selection query
    Arrays.fill(columnDataTypes, ColumnDataType.STRING);
    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
    return new SelectionResultsBlock(dataSchema, Collections.emptyList());
  }

  private static AggregationResultsBlock buildEmptyAggregationQueryResults(QueryContext queryContext) {
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    int numAggregations = aggregationFunctions.length;
    List<Object> results = new ArrayList<>(numAggregations);
    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      results.add(aggregationFunction.extractAggregationResult(aggregationFunction.createAggregationResultHolder()));
    }
    return new AggregationResultsBlock(aggregationFunctions, results);
  }

  private static GroupByResultsBlock buildEmptyGroupByQueryResults(QueryContext queryContext) {
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    int numAggregations = aggregationFunctions.length;
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    assert groupByExpressions != null;
    int numColumns = groupByExpressions.size() + numAggregations;
    String[] columnNames = new String[numColumns];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numColumns];
    int index = 0;
    for (ExpressionContext groupByExpression : groupByExpressions) {
      columnNames[index] = groupByExpression.toString();
      // Use STRING column data type as default for group-by expressions
      columnDataTypes[index] = ColumnDataType.STRING;
      index++;
    }
    for (AggregationFunction aggregationFunction : aggregationFunctions) {
      // NOTE: Use AggregationFunction.getResultColumnName() for SQL format response
      columnNames[index] = aggregationFunction.getResultColumnName();
      columnDataTypes[index] = aggregationFunction.getIntermediateResultColumnType();
      index++;
    }
    return new GroupByResultsBlock(new DataSchema(columnNames, columnDataTypes));
  }

  private static DistinctResultsBlock buildEmptyDistinctQueryResults(QueryContext queryContext) {
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assert aggregationFunctions != null && aggregationFunctions.length == 1
        && aggregationFunctions[0] instanceof DistinctAggregationFunction;
    DistinctAggregationFunction distinctAggregationFunction = (DistinctAggregationFunction) aggregationFunctions[0];
    String[] columnNames = distinctAggregationFunction.getColumns();
    ColumnDataType[] columnDataTypes = new ColumnDataType[columnNames.length];
    // NOTE: Use STRING column data type as default for distinct query
    Arrays.fill(columnDataTypes, ColumnDataType.STRING);
    DistinctTable distinctTable =
        new DistinctTable(new DataSchema(columnNames, columnDataTypes), Collections.emptySet(),
            queryContext.isNullHandlingEnabled());
    return new DistinctResultsBlock(distinctAggregationFunction, distinctTable);
  }
}
