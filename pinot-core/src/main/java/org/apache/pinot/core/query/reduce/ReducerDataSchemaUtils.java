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
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;


@SuppressWarnings("rawtypes")
public class ReducerDataSchemaUtils {
  private ReducerDataSchemaUtils() {
  }

  /**
   * Returns the canonical data schema of the aggregation result based on the query and the data schema returned from
   * the server.
   * <p>Column names are re-generated in the canonical data schema to avoid the backward incompatibility caused by
   * changing the string representation of the expression.
   */
  public static DataSchema canonicalizeDataSchemaForAggregation(QueryContext queryContext, DataSchema dataSchema) {
    List<Pair<AggregationFunction, FilterContext>> filteredAggregationFunctions =
        queryContext.getFilteredAggregationFunctions();
    assert filteredAggregationFunctions != null;
    int numAggregations = filteredAggregationFunctions.size();
    Preconditions.checkState(dataSchema.size() == numAggregations,
        "BUG: Expect same number of aggregations and columns in data schema, got %s aggregations, %s columns in data "
            + "schema", numAggregations, dataSchema.size());
    String[] columnNames = new String[numAggregations];
    for (int i = 0; i < numAggregations; i++) {
      Pair<AggregationFunction, FilterContext> pair = filteredAggregationFunctions.get(i);
      AggregationFunction aggregationFunction = pair.getLeft();
      columnNames[i] = AggregationFunctionUtils.getResultColumnName(aggregationFunction, pair.getRight());
    }
    return new DataSchema(columnNames, dataSchema.getColumnDataTypes());
  }

  /**
   * Returns the canonical data schema of the group-by result based on the query and the data schema returned from the
   * server. Group-by expressions are always at the beginning of the data schema, followed by the aggregations.
   * <p>Column names are re-generated in the canonical data schema to avoid the backward incompatibility caused by
   * changing the string representation of the expression.
   */
  public static DataSchema canonicalizeDataSchemaForGroupBy(QueryContext queryContext, DataSchema dataSchema) {
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    List<Pair<AggregationFunction, FilterContext>> filteredAggregationFunctions =
        queryContext.getFilteredAggregationFunctions();
    assert groupByExpressions != null && filteredAggregationFunctions != null;
    int numGroupByExpression = groupByExpressions.size();
    int numAggregations = filteredAggregationFunctions.size();
    int numColumns = numGroupByExpression + numAggregations;
    String[] columnNames = new String[numColumns];
    Preconditions.checkState(dataSchema.size() == numColumns,
        "BUG: Expect same number of group-by expressions, aggregations and columns in data schema, got %s group-by "
            + "expressions, %s aggregations, %s columns in data schema", numGroupByExpression, numAggregations,
        dataSchema.size());
    for (int i = 0; i < numGroupByExpression; i++) {
      columnNames[i] = groupByExpressions.get(i).toString();
    }
    for (int i = 0; i < numAggregations; i++) {
      Pair<AggregationFunction, FilterContext> pair = filteredAggregationFunctions.get(i);
      columnNames[numGroupByExpression + i] =
          AggregationFunctionUtils.getResultColumnName(pair.getLeft(), pair.getRight());
    }
    return new DataSchema(columnNames, dataSchema.getColumnDataTypes());
  }

  /**
   * Returns the canonical data schema of the distinct result based on the query and the data schema returned from the
   * server.
   * <p>Column names are re-generated in the canonical data schema to avoid the backward incompatibility caused by
   * changing the string representation of the expression.
   */
  public static DataSchema canonicalizeDataSchemaForDistinct(QueryContext queryContext, DataSchema dataSchema) {
    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    int numSelectExpressions = selectExpressions.size();
    Preconditions.checkState(dataSchema.size() == numSelectExpressions,
        "BUG: Expect same number of columns in SELECT clause and data schema, got %s in SELECT clause, %s in data "
            + "schema", numSelectExpressions, dataSchema.size());
    String[] columnNames = new String[numSelectExpressions];
    for (int i = 0; i < numSelectExpressions; i++) {
      columnNames[i] = selectExpressions.get(i).toString();
    }
    return new DataSchema(columnNames, dataSchema.getColumnDataTypes());
  }
}
