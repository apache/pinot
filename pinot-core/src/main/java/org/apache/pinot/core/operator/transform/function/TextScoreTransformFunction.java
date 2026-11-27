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
package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.MultiColumnTextIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextScoreStats;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Implements `TEXT_SCORE(column, 'search query'[, 'options'[, 'globalStats']])`: the relevance score of each
/// document for a text search query, read from the column's text index (Lucene BM25 by default). Documents that do
/// not match the query score `0`.
///
/// Matching documents receive Lucene's score, which can also be zero for a zero-boost query. Use
/// `TEXT_MATCH` to test membership. Scoring requires a per-row index on a single-valued STRING column.
/// Queries with row-level security filters cannot use index-wide scores.
///
/// A per-segment [TextIndexReader.DocScorer] is created lazily on the first non-empty block and scores only the
/// document IDs present in each incoming [ValueBlock], avoiding materializing a segment-wide score map for selective
/// filters, `LIMIT` queries, or second-pass `ORDER BY ... LIMIT` scoring.
///
/// By default scores use per-segment term statistics (IDF, average field length), so they rank documents correctly
/// within a segment but are only approximately comparable across segments.
///
/// With the optional 4th argument, the serialized [TextScoreStats] returned by
/// `TEXT_SCORE_STATS(column, 'search query'[, 'options'])` for the same query, every segment scores with those
/// corpus-wide statistics instead, so equal documents get equal scores in every segment (two-step search, like
/// Elasticsearch's `dfs_query_then_fetch`). An empty string means no global statistics. Pass `''` as options to use
/// the default parser with global statistics. Global scoring supports term and phrase queries, Boolean combinations,
/// boosts and uncapped constant-score rewrites. Queries with segment-specific expansions such as fuzzy queries use
/// local scoring. Repeat the statistics query if the supplied counters have become inconsistent with the index.
///
/// Not thread-safe: like other transform functions, an instance serves one segment on one thread.
public class TextScoreTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "textScore";

  private TextIndexReader _textIndexReader;
  /// Set only when the column is served by a multi-column text index.
  @Nullable
  private String _multiColumnName;
  private String _searchQuery;
  @Nullable
  private String _options;
  @Nullable
  private TextScoreStats _globalStats;
  @Nullable
  private TextIndexReader.DocScorer _docScorer;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap,
      QueryContext queryContext) {
    if (queryContext.getRowFilter() != null) {
      throw new IllegalArgumentException("TEXT_SCORE is not supported when row-level security filters apply");
    }
    super.init(arguments, columnContextMap, queryContext);
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    int numArguments = arguments.size();
    if (numArguments < 2 || numArguments > 4) {
      throw new IllegalArgumentException("Expected 2 to 4 arguments for TEXT_SCORE: "
          + "TEXT_SCORE(column, 'searchQuery'[, 'options'[, 'globalStats']])");
    }

    TransformFunction columnArg = arguments.get(0);
    if (!(columnArg instanceof IdentifierTransformFunction identifier)) {
      throw new IllegalArgumentException("The first argument of TEXT_SCORE must be a column with a text index");
    }
    String columnName = identifier.getColumnName();
    DataSource dataSource = columnContextMap.get(columnName).getDataSource();
    TextIndexReader textIndexReader = dataSource != null ? dataSource.getTextIndex() : null;
    if (textIndexReader == null && dataSource != null) {
      textIndexReader = dataSource.getMultiColumnTextIndex();
      _multiColumnName = columnName;
    }
    if (textIndexReader == null) {
      throw new IllegalArgumentException("Cannot apply TEXT_SCORE on column: " + columnName + " without text index");
    }
    TransformResultMetadata metadata = identifier.getResultMetadata();
    if (!metadata.isSingleValue() || metadata.getDataType() != DataType.STRING) {
      throw new IllegalArgumentException("TEXT_SCORE requires a single-valued STRING column: " + columnName);
    }
    _textIndexReader = textIndexReader;

    _searchQuery = getStringLiteral(arguments.get(1), "second");
    _options = numArguments >= 3 ? getStringLiteral(arguments.get(2), "third") : null;
    if (numArguments == 4) {
      String globalStats = getStringLiteral(arguments.get(3), "fourth");
      if (!globalStats.isBlank()) {
        _globalStats = TextScoreStats.fromJson(globalStats);
      }
    }
  }

  private static String getStringLiteral(TransformFunction argument, String position) {
    if (!(argument instanceof LiteralTransformFunction && argument.getResultMetadata().isSingleValue()
        && argument.getResultMetadata().getDataType() == DataType.STRING)) {
      throw new IllegalArgumentException(
          "The " + position + " argument of TEXT_SCORE must be a single-valued string literal");
    }
    return ((LiteralTransformFunction) argument).getStringLiteral();
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public double[] transformToDoubleValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initDoubleValuesSV(length);
    if (length == 0) {
      return _doubleValuesSV;
    }
    getDocScorer().scoreDocs(valueBlock.getDocIds(), length, _doubleValuesSV);
    return _doubleValuesSV;
  }

  private TextIndexReader.DocScorer getDocScorer() {
    if (_docScorer == null) {
      _docScorer = _multiColumnName != null
          ? ((MultiColumnTextIndexReader) _textIndexReader).createDocScorer(_multiColumnName, _searchQuery, _options,
              _globalStats)
          : _textIndexReader.createDocScorer(_searchQuery, _options, _globalStats);
    }
    return _docScorer;
  }
}
