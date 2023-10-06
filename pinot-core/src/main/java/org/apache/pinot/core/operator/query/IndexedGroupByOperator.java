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
package org.apache.pinot.core.operator.query;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.InvertedIndexDataFetcher;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.aggregation.groupby.InvertedGroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * todo
 */
public class IndexedGroupByOperator extends BaseOperator<GroupByResultsBlock> {
  private static final String EXPLAIN_NAME = "INDEXED_GROUP_BY";

  private final QueryContext _queryContext;
  private final BaseFilterOperator _filterOperator;
  private final InvertedIndexDataFetcher _invertedIndexDataFetcher;
  private final String _columnName;
  private final DataSchema _dataSchema;
  private final AggregationFunction[] _aggregationFunctions;
  private final ExpressionContext[] _groupByExpressions;

  public IndexedGroupByOperator(QueryContext queryContext, ExpressionContext[] groupByExpressions,
      BaseFilterOperator filterOperator, Map<String, DataSource> dataSourceMap) {
    Preconditions.checkArgument(filterOperator instanceof MatchAllFilterOperator, "Don't support filters yet");
    // Preconditions.checkArgument(filterOperator.canProduceBitmaps());
    _queryContext = queryContext;
    _filterOperator = filterOperator;
    _invertedIndexDataFetcher = new InvertedIndexDataFetcher(dataSourceMap);
    _columnName = groupByExpressions[0].getIdentifier();

    _aggregationFunctions = queryContext.getAggregationFunctions();
    _groupByExpressions = groupByExpressions;

    Preconditions.checkNotNull(_aggregationFunctions);
    int numAggregationFunctions = _aggregationFunctions.length;
    int numGroupByExpressions = groupByExpressions.length;
    int numResultColumns = numAggregationFunctions + numGroupByExpressions;
    String[] columnNames = new String[numResultColumns];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numResultColumns];
    for (int i = 0; i < numGroupByExpressions; i++) {
      columnNames[i] = _groupByExpressions[i].getIdentifier();
      // TODO: Remove hardcoded type
      columnDataTypes[i] = DataSchema.ColumnDataType.STRING;
    }
    // Extract column names and data types for aggregation functions
    for (int i = 0; i < numAggregationFunctions; i++) {
      int index = numGroupByExpressions + i;
      Pair<AggregationFunction, FilterContext> pair = queryContext.getFilteredAggregationFunctions().get(i);
      AggregationFunction aggregationFunction = pair.getLeft();
      String columnName = AggregationFunctionUtils.getResultColumnName(aggregationFunction, pair.getRight());
      columnNames[index] = columnName;
      columnDataTypes[index] = aggregationFunction.getIntermediateResultColumnType();
    }

    _dataSchema = new DataSchema(columnNames, columnDataTypes);
  }

  @Override
  public List<BaseFilterOperator> getChildOperators() {
    return Collections.singletonList(_filterOperator);
  }

  @Override
  protected GroupByResultsBlock getNextBlock() {
    // - [x] Get values from dictionary
    // - [x] Get docIds for each dictId from inverted index
    // - [x] GroupKeyGenerator returns an iterator for group-keys
    // - [x] GroupResultHolder stores results
    // - [x] Create AggregationGroupByResult
    // - [x] Create GroupByResultsBlock
    Object[] values = _invertedIndexDataFetcher.getValues(_columnName);
    GroupKeyGenerator groupKeyGenerator = new InvertedGroupKeyGenerator(values);
    GroupByResultHolder groupByResultHolder = new DoubleGroupByResultHolder(values.length, values.length, 0);
    for (int dictId = 0; dictId < values.length; dictId++) {
      ImmutableRoaringBitmap bitmap = _invertedIndexDataFetcher.getDocIds(_columnName, dictId);
      groupByResultHolder.setValueForKey(dictId, (double) bitmap.getCardinality());
    }
    AggregationGroupByResult groupByResult = new AggregationGroupByResult(
        groupKeyGenerator, _aggregationFunctions, new GroupByResultHolder[]{groupByResultHolder});
    return new GroupByResultsBlock(_dataSchema, groupByResult, _queryContext);
  }

  @Nullable
  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(groupKeys:");
    if (_groupByExpressions.length > 0) {
      stringBuilder.append(_groupByExpressions[0].toString());
      for (int i = 1; i < _groupByExpressions.length; i++) {
        stringBuilder.append(", ").append(_groupByExpressions[i].toString());
      }
    }

    stringBuilder.append(", aggregations:");
    if (_aggregationFunctions.length > 0) {
      stringBuilder.append(_aggregationFunctions[0].toExplainString());
      for (int i = 1; i < _aggregationFunctions.length; i++) {
        stringBuilder.append(", ").append(_aggregationFunctions[i].toExplainString());
      }
    }

    return stringBuilder.append(')').toString();
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return new ExecutionStatistics(0, 0, 0, 0);
  }
}
