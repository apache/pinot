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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.postaggregation.PostAggregationFunction;
import org.apache.pinot.core.query.reduce.filter.ColumnValueExtractor;
import org.apache.pinot.core.query.reduce.filter.LiteralValueExtractor;
import org.apache.pinot.core.query.reduce.filter.ValueExtractor;
import org.apache.pinot.core.query.reduce.filter.ValueExtractorFactory;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * The {@code PostAggregationHandler} handles the post-aggregation calculation as well as the column re-ordering for the
 * aggregation result.
 */
public class PostAggregationHandler implements ValueExtractorFactory {
  private final Map<Pair<FunctionContext, FilterContext>, Integer> _filteredAggregationsIndexMap;
  private final int _numGroupByExpressions;
  private final Map<ExpressionContext, Integer> _groupByExpressionIndexMap;
  private final DataSchema _dataSchema;
  private final ValueExtractor[] _valueExtractors;
  private final DataSchema _resultDataSchema;

  public PostAggregationHandler(QueryContext queryContext, DataSchema dataSchema) {
    _filteredAggregationsIndexMap = queryContext.getFilteredAggregationsIndexMap();
    assert _filteredAggregationsIndexMap != null;
    List<ExpressionContext> groupByExpressions = queryContext.getGroupByExpressions();
    if (groupByExpressions != null) {
      _numGroupByExpressions = groupByExpressions.size();
      _groupByExpressionIndexMap = new HashMap<>();
      for (int i = 0; i < _numGroupByExpressions; i++) {
        _groupByExpressionIndexMap.put(groupByExpressions.get(i), i);
      }
    } else {
      _numGroupByExpressions = 0;
      _groupByExpressionIndexMap = null;
    }

    // NOTE: The data schema will always have group-by expressions in the front, followed by aggregation functions of
    //       the same order as in the query context. This is handled in AggregationGroupByOrderByOperator.
    _dataSchema = dataSchema;

    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    int numSelectExpressions = selectExpressions.size();
    _valueExtractors = new ValueExtractor[numSelectExpressions];
    String[] columnNames = new String[numSelectExpressions];
    ColumnDataType[] columnDataTypes = new ColumnDataType[numSelectExpressions];
    for (int i = 0; i < numSelectExpressions; i++) {
      ValueExtractor valueExtractor = getValueExtractor(selectExpressions.get(i));
      _valueExtractors[i] = valueExtractor;
      columnNames[i] = valueExtractor.getColumnName();
      columnDataTypes[i] = valueExtractor.getColumnDataType();
    }
    _resultDataSchema = new DataSchema(columnNames, columnDataTypes);
  }

  /**
   * Returns the DataSchema of the post-aggregation result.
   */
  public DataSchema getResultDataSchema() {
    return _resultDataSchema;
  }

  /**
   * Returns the post-aggregation result for the given row.
   */
  public Object[] getResult(Object[] row) {
    int numValues = _valueExtractors.length;
    Object[] result = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      result[i] = _valueExtractors[i].extract(row);
    }
    return result;
  }

  /**
   * Returns a ValueExtractor based on the given expression.
   */
  @Override
  public ValueExtractor getValueExtractor(ExpressionContext expression) {
    if (expression.getType() == ExpressionContext.Type.LITERAL) {
      // Literal
      return new LiteralValueExtractor(expression.getLiteral().getStringValue());
    }
    if (_numGroupByExpressions > 0) {
      Integer groupByExpressionIndex = _groupByExpressionIndexMap.get(expression);
      if (groupByExpressionIndex != null) {
        // Group-by expression
        return new ColumnValueExtractor(groupByExpressionIndex, _dataSchema);
      }
    }
    FunctionContext function = expression.getFunction();
    Preconditions
        .checkState(function != null, "Failed to find SELECT expression: %s in the GROUP-BY clause", expression);
    if (function.getType() == FunctionContext.Type.AGGREGATION) {
      // Aggregation function
      return new ColumnValueExtractor(
          _filteredAggregationsIndexMap.get(Pair.of(function, null)) + _numGroupByExpressions, _dataSchema);
    } else if (function.getType() == FunctionContext.Type.TRANSFORM && function.getFunctionName()
        .equalsIgnoreCase("filter")) {
      FunctionContext aggregation = function.getArguments().get(0).getFunction();
      ExpressionContext filterExpression = function.getArguments().get(1);
      FilterContext filter = RequestContextUtils.getFilter(filterExpression);
      return new ColumnValueExtractor(
          _filteredAggregationsIndexMap.get(Pair.of(aggregation, filter)) + _numGroupByExpressions, _dataSchema);
    } else {
      // Post-aggregation function
      return new PostAggregationValueExtractor(function);
    }
  }

  /**
   * Value extractor for a post-aggregation column.
   */
  private class PostAggregationValueExtractor implements ValueExtractor {
    final FunctionContext _function;
    final Object[] _arguments;
    final ValueExtractor[] _argumentExtractors;
    final PostAggregationFunction _postAggregationFunction;

    PostAggregationValueExtractor(FunctionContext function) {
      assert function.getType() == FunctionContext.Type.TRANSFORM;

      _function = function;
      List<ExpressionContext> arguments = function.getArguments();
      int numArguments = arguments.size();
      _arguments = new Object[numArguments];
      _argumentExtractors = new ValueExtractor[numArguments];
      ColumnDataType[] argumentTypes = new ColumnDataType[numArguments];
      for (int i = 0; i < numArguments; i++) {
        ExpressionContext argument = arguments.get(i);
        ValueExtractor argumentExtractor = getValueExtractor(argument);
        _argumentExtractors[i] = argumentExtractor;
        argumentTypes[i] = argumentExtractor.getColumnDataType();
      }
      _postAggregationFunction = new PostAggregationFunction(function.getFunctionName(), argumentTypes);
    }

    @Override
    public String getColumnName() {
      return _function.toString();
    }

    @Override
    public ColumnDataType getColumnDataType() {
      return _postAggregationFunction.getResultType();
    }

    @Override
    public Object extract(Object[] row) {
      int numArguments = _arguments.length;
      for (int i = 0; i < numArguments; i++) {
        _arguments[i] = _argumentExtractors[i].extract(row);
      }
      return _postAggregationFunction.invoke(_arguments);
    }
  }
}
