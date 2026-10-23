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

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.MultiColumnTextIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Implements `TEXT_SCORE(column, 'search query'[, 'options'])`: the relevance score of each document for a text
/// search query, read from the column's text index (Lucene BM25 by default). Documents that do not match the query
/// score `0`.
///
/// The query is parsed exactly like `TEXT_MATCH` with the same arguments, so
/// `TEXT_SCORE(col, q) > 0` holds exactly for the documents `TEXT_MATCH(col, q)` accepts (BM25 scores of matching
/// documents are strictly positive; constant-score queries such as wildcards score `1`).
///
/// The index is searched once per segment, on the first block, and the doc id to score map is reused for the
/// remaining blocks of that segment. Scores use per-segment term statistics (IDF, average field length), so they
/// rank documents correctly within a segment but are only approximately comparable across segments.
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
  private Int2FloatMap _scores;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    int numArguments = arguments.size();
    if (numArguments < 2 || numArguments > 3) {
      throw new IllegalArgumentException(
          "Expected 2 or 3 arguments for TEXT_SCORE: TEXT_SCORE(column, 'searchQuery'[, 'options'])");
    }

    TransformFunction columnArg = arguments.get(0);
    if (!(columnArg instanceof IdentifierTransformFunction && columnArg.getResultMetadata().isSingleValue())) {
      throw new IllegalArgumentException("The first argument of TEXT_SCORE must be a single-valued column");
    }
    String columnName = ((IdentifierTransformFunction) columnArg).getColumnName();
    DataSource dataSource = columnContextMap.get(columnName).getDataSource();
    TextIndexReader textIndexReader = dataSource != null ? dataSource.getTextIndex() : null;
    if (textIndexReader == null && dataSource != null) {
      textIndexReader = dataSource.getMultiColumnTextIndex();
      _multiColumnName = columnName;
    }
    if (textIndexReader == null) {
      throw new IllegalArgumentException("Cannot apply TEXT_SCORE on column: " + columnName + " without text index");
    }
    _textIndexReader = textIndexReader;

    _searchQuery = getStringLiteral(arguments.get(1), "second");
    _options = numArguments == 3 ? getStringLiteral(arguments.get(2), "third") : null;
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
    Int2FloatMap scores = getScores();
    int[] docIds = valueBlock.getDocIds();
    for (int i = 0; i < length; i++) {
      // Int2FloatMap returns its default (0) for documents that do not match
      _doubleValuesSV[i] = scores.get(docIds[i]);
    }
    return _doubleValuesSV;
  }

  private Int2FloatMap getScores() {
    if (_scores == null) {
      _scores = _multiColumnName != null
          ? ((MultiColumnTextIndexReader) _textIndexReader).getDocScores(_multiColumnName, _searchQuery, _options)
          : _textIndexReader.getDocScores(_searchQuery, _options);
    }
    return _scores;
  }
}
