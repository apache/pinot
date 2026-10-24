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
package org.apache.pinot.core.query.aggregation.function;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.MultiColumnTextIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextScoreStats;


/// Implements `TEXT_SCORE_STATS(column, 'search query'[, 'options'])`: the corpus statistics BM25 needs for the
/// query, summed over every queried segment, serialized as [TextScoreStats] JSON. Passing the result as the 4th
/// argument of `TEXT_SCORE(column, 'search query', 'options', '<stats>')` makes every segment score with
/// corpus-wide document frequencies and average field length (two-step search, like Elasticsearch's
/// `dfs_query_then_fetch`).
///
/// The query and options are parsed exactly as `TEXT_SCORE` parses them, so the statistics cover the same analyzed
/// terms. Each segment's statistics are read from its text index metadata without scanning documents (see
/// `AggregationPlanNode`); the filter does not restrict them, only segment pruning does. The function must be the
/// only aggregation of its query and does not support GROUP BY.
///
/// Intermediate and final results are the JSON string; [#merge] sums the counters.
public class TextScoreStatsAggregationFunction extends BaseSingleInputAggregationFunction<String, String> {
  private static final String EMPTY_STATS = new TextScoreStats().toJson();

  private final String _searchQuery;
  @Nullable
  private final String _options;

  public TextScoreStatsAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments.get(0), nullHandlingEnabled);
    int numArguments = arguments.size();
    Preconditions.checkArgument(numArguments == 2 || numArguments == 3,
        "TEXT_SCORE_STATS expects 2 or 3 arguments: TEXT_SCORE_STATS(column, 'searchQuery'[, 'options'])");
    Preconditions.checkArgument(arguments.get(0).getType() == ExpressionContext.Type.IDENTIFIER,
        "The first argument of TEXT_SCORE_STATS must be a column with a text index");
    _searchQuery = getStringLiteral(arguments.get(1), "second");
    _options = numArguments == 3 ? getStringLiteral(arguments.get(2), "third") : null;
  }

  private static String getStringLiteral(ExpressionContext argument, String position) {
    Preconditions.checkArgument(argument.getType() == ExpressionContext.Type.LITERAL,
        "The %s argument of TEXT_SCORE_STATS must be a string literal", position);
    return argument.getLiteral().getStringValue();
  }

  /// Returns the serialized statistics of one segment, read from the column's (single- or multi-column) text index.
  public String getSegmentStats(DataSource dataSource) {
    String column = _expression.getIdentifier();
    TextIndexReader textIndexReader = dataSource.getTextIndex();
    if (textIndexReader != null) {
      return textIndexReader.getTextScoreStats(_searchQuery, _options).toJson();
    }
    MultiColumnTextIndexReader multiColumnTextIndex = dataSource.getMultiColumnTextIndex();
    if (multiColumnTextIndex != null) {
      return multiColumnTextIndex.getTextScoreStats(column, _searchQuery, _options).toJson();
    }
    throw new IllegalArgumentException("Cannot apply TEXT_SCORE_STATS on column: " + column + " without text index");
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.TEXTSCORESTATS;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    // Only used to build the empty result when no segment is processed (e.g. all segments pruned)
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    throw unsupportedScan();
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    throw unsupportedScan();
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    throw unsupportedScan();
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    throw unsupportedScan();
  }

  /// Statistics come from index metadata, never from scanning rows; this path is reached only for query shapes the
  /// function does not support.
  private static UnsupportedOperationException unsupportedScan() {
    return new UnsupportedOperationException(
        "TEXT_SCORE_STATS must be the only aggregation function of a query without GROUP BY or FILTER clauses");
  }

  @Override
  public String extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    String result = aggregationResultHolder.getResult();
    return result != null ? result : EMPTY_STATS;
  }

  @Override
  public String extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    throw unsupportedScan();
  }

  @Override
  public String merge(String intermediateResult1, String intermediateResult2) {
    if (intermediateResult1 == null || intermediateResult1.isEmpty()) {
      return intermediateResult2;
    }
    if (intermediateResult2 == null || intermediateResult2.isEmpty()) {
      return intermediateResult1;
    }
    return TextScoreStats.fromJson(intermediateResult1).merge(TextScoreStats.fromJson(intermediateResult2)).toJson();
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public String extractFinalResult(@Nullable String intermediateResult) {
    // No segment queried (e.g. all pruned): empty statistics, which TEXT_SCORE treats as "no global statistics"
    return intermediateResult != null ? intermediateResult : EMPTY_STATS;
  }

  @Override
  public String mergeFinalResult(String finalResult1, String finalResult2) {
    return merge(finalResult1, finalResult2);
  }
}
