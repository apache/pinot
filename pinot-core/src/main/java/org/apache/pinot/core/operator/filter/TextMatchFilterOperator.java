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
package org.apache.pinot.core.operator.filter;

import com.google.common.base.CaseFormat;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.spi.index.reader.MultiColumnTextIndexReader;
import org.apache.pinot.segment.spi.index.reader.QueryScopedCache;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.spi.trace.FilterType;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/// Filter operator for supporting the execution of text search
/// queries: WHERE TEXT_MATCH(column_name, query_string, options_string)
public class TextMatchFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_TEXT_INDEX";

  // name of text column to query, used with multi-column text indexes.
  private final String _column;
  private final TextIndexReader _textIndexReader;
  private final TextMatchPredicate _predicate;
  /// Lets the segments of one request share a parsed query instead of each parsing it again. Null
  /// when no query scope is available, which simply means no sharing.
  private final QueryScopedCache _queryScopedCache;

  public TextMatchFilterOperator(String column, TextIndexReader textIndexReader, TextMatchPredicate predicate,
      int numDocs) {
    this(column, textIndexReader, predicate, numDocs, null);
  }

  public TextMatchFilterOperator(String column, TextIndexReader textIndexReader, TextMatchPredicate predicate,
      int numDocs, @Nullable QueryScopedCache queryScopedCache) {
    super(getSearchableDocCount(textIndexReader, numDocs), false);
    _column = column;
    _textIndexReader = textIndexReader;
    _predicate = predicate;
    _queryScopedCache = queryScopedCache;
  }

  private static int getSearchableDocCount(TextIndexReader reader, int numDocs) {
    int searchableDocCount = reader.getSearchableDocCount();
    return searchableDocCount >= 0 && searchableDocCount < numDocs ? searchableDocCount : numDocs;
  }

  public TextMatchFilterOperator(TextIndexReader textIndexReader, TextMatchPredicate predicate, int numDocs) {
    this(null, textIndexReader, predicate, numDocs, null);
  }

  public TextMatchFilterOperator(TextIndexReader textIndexReader, TextMatchPredicate predicate, int numDocs,
      @Nullable QueryScopedCache queryScopedCache) {
    this(null, textIndexReader, predicate, numDocs, queryScopedCache);
  }

  @Override
  protected BlockDocIdSet getTrues() {
    if (_textIndexReader.isMultiColumn()) {
      return BitmapDocIdSet.create(
          ((MultiColumnTextIndexReader) _textIndexReader).getDocIds(_column, _predicate.getValue(),
              _predicate.getOptions()), _numDocs);
    } else {
      return BitmapDocIdSet.create(_textIndexReader.getDocIdsWithParseCache(_predicate.getValue(),
          _predicate.getOptions(), _queryScopedCache), _numDocs);
    }
  }

  @Override
  public boolean canOptimizeCount() {
    return true;
  }

  @Override
  public int getNumMatchingDocs() {
    if (_textIndexReader.isMultiColumn()) {
      return ((MultiColumnTextIndexReader) _textIndexReader).getNumMatchingDocs(_column, _predicate.getValue(),
          _predicate.getOptions());
    } else {
      // Ask the index for the size of the match set rather than for the match set itself: building a bitmap of
      // every matching doc just to read its cardinality is the dominant cost of a count over a frequent term.
      return _textIndexReader.getNumMatchingDocsWithParseCache(_predicate.getValue(), _predicate.getOptions(),
          _queryScopedCache);
    }
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  public BitmapCollection getBitmaps() {
    ImmutableRoaringBitmap bitmap = _textIndexReader.isMultiColumn()
        ? ((MultiColumnTextIndexReader) _textIndexReader).getDocIds(_column, _predicate.getValue(),
        _predicate.getOptions())
        : _textIndexReader.getDocIdsWithParseCache(_predicate.getValue(), _predicate.getOptions(), _queryScopedCache);

    record(bitmap);
    return new BitmapCollection(_numDocs, false, bitmap);
  }

  @Override
  public List<Operator> getChildOperators() {
    return List.of();
  }

  @Override
  public String toExplainString() {
    StringBuilder sb = new StringBuilder(EXPLAIN_NAME).append("(indexLookUp:text_index");
    sb.append(",operator:").append(_predicate.getType());
    if (_column != null) {
      // column is already included in predicate
      sb.append(",multiColumnIndex:true");
    }
    sb.append(",predicate:").append(_predicate);
    return sb.append(')').toString();
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder builder) {
    super.explainAttributes(builder);
    builder.putString("indexLookUp", "text_index");
    builder.putString("operator", _predicate.getType().name());
    if (_column != null) {
      builder.putString("multiColumnIndex", "true");
    }
    builder.putString("predicate", _predicate.toString());
  }

  private void record(ImmutableRoaringBitmap matches) {
    InvocationRecording recording = Tracing.activeRecording();
    if (recording.isEnabled()) {
      recording.setNumDocsMatchingAfterFilter(matches.getCardinality());
      recording.setColumnName(_predicate.getLhs().getIdentifier());
      recording.setFilter(FilterType.INDEX, "LUCENE_TEXT");
    }
  }
}
