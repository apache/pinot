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
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.MutableRoaringBitmapUnion;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.spi.trace.FilterType;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class InvertedIndexFilterOperator extends BaseColumnFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_INVERTED_INDEX";

  private final PredicateEvaluator _predicateEvaluator;
  private final InvertedIndexReader<ImmutableRoaringBitmap> _invertedIndexReader;
  private final boolean _exclusive;
  private final boolean _isSingleValue;

  InvertedIndexFilterOperator(QueryContext queryContext, PredicateEvaluator predicateEvaluator, DataSource dataSource,
      int numDocs) {
    super(queryContext, dataSource, numDocs);
    _predicateEvaluator = predicateEvaluator;
    @SuppressWarnings("unchecked")
    InvertedIndexReader<ImmutableRoaringBitmap> invertedIndexReader =
        (InvertedIndexReader<ImmutableRoaringBitmap>) dataSource.getInvertedIndex();
    _invertedIndexReader = invertedIndexReader;
    _exclusive = predicateEvaluator.isExclusive();
    _isSingleValue = dataSource.getDataSourceMetadata().isSingleValue();
  }

  @Override
  protected BlockDocIdSet getNextBlockWithoutNullHandling() {
    int[] dictIds = _exclusive ? _predicateEvaluator.getNonMatchingDictIds() : _predicateEvaluator.getMatchingDictIds();
    int numDictIds = dictIds.length;
    if (numDictIds == 0) {
      return EmptyDocIdSet.unscanned();
    }
    if (numDictIds == 1) {
      ImmutableRoaringBitmap docIds = getDocIds(dictIds[0]);
      if (_exclusive) {
        if (docIds instanceof MutableRoaringBitmap) {
          MutableRoaringBitmap mutableRoaringBitmap = (MutableRoaringBitmap) docIds;
          mutableRoaringBitmap.flip(0L, _numDocs);
          return BitmapDocIdSet.create(mutableRoaringBitmap, _numDocs);
        } else {
          return BitmapDocIdSet.create(ImmutableRoaringBitmap.flip(docIds, 0L, _numDocs), _numDocs);
        }
      } else {
        return BitmapDocIdSet.create(docIds, _numDocs);
      }
    } else {
      ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[numDictIds];
      for (int i = 0; i < numDictIds; i++) {
        bitmaps[i] = getDocIds(dictIds[i]);
      }
      MutableRoaringBitmap docIds = ImmutableRoaringBitmap.or(bitmaps);
      if (_exclusive) {
        docIds.flip(0L, _numDocs);
      }
      InvocationRecording recording = Tracing.activeRecording();
      if (recording.isEnabled()) {
        recording.setColumnName(_predicateEvaluator.getPredicate().getLhs().getIdentifier());
        recording.setNumDocsMatchingAfterFilter(docIds.getCardinality());
        recording.setFilter(FilterType.INDEX, String.valueOf(_predicateEvaluator.getPredicateType()));
      }
      return BitmapDocIdSet.create(docIds, _numDocs);
    }
  }

  @Override
  public boolean canOptimizeCount() {
    return true;
  }

  @Override
  public int getNumMatchingDocs() {
    ImmutableRoaringBitmap nullBitmap = getNullBitmap();
    if (nullBitmap != null && !_isSingleValue) {
      // Per-dictId bitmaps overlap on a multi-value column, so the null rows among the matches can only be counted on
      // the materialized union
      return getBitmaps().getCardinality();
    }
    int[] dictIds = _exclusive ? _predicateEvaluator.getNonMatchingDictIds() : _predicateEvaluator.getMatchingDictIds();
    int count;
    // Null rows among the matches are UNKNOWN rather than true
    int numNulls = 0;
    if (_isSingleValue) {
      // On a single-value column, per-dictId bitmaps partition the docId space (each docId has exactly one
      // dictId), so the union cardinality equals the sum of per-bitmap cardinalities. No scratch bitmap is
      // allocated and no OR pass is performed.
      count = 0;
      if (nullBitmap == null) {
        for (int dictId : dictIds) {
          count += getDocIds(dictId).getCardinality();
        }
      } else {
        for (int dictId : dictIds) {
          ImmutableRoaringBitmap docIds = getDocIds(dictId);
          count += docIds.getCardinality();
          numNulls += ImmutableRoaringBitmap.andCardinality(docIds, nullBitmap);
        }
      }
    } else {
      // TODO: For MV column, per-dictId bitmaps may overlap, so we must materialize the union to count.
      // A streaming union-cardinality variant was benchmarked but not implemented as a few combinations of
      // cardinality and number of matching dictIds shows regressions.
      count = 0;
      switch (dictIds.length) {
        case 0:
          break;
        case 1:
          count = getDocIds(dictIds[0]).getCardinality();
          break;
        case 2:
          count = ImmutableRoaringBitmap.orCardinality(getDocIds(dictIds[0]), getDocIds(dictIds[1]));
          break;
        default:
          // Union lazily and finalize once at the end: the fold still materializes the union, but skips per-union
          // cardinality maintenance
          MutableRoaringBitmapUnion union = new MutableRoaringBitmapUnion();
          for (int dictId : dictIds) {
            union.add(getDocIds(dictId));
          }
          count = union.get().getCardinality();
          break;
      }
    }
    return toNumTrueDocs(count, numNulls, _exclusive);
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  public BitmapCollection getBitmaps() {
    int[] dictIds = _exclusive ? _predicateEvaluator.getNonMatchingDictIds() : _predicateEvaluator.getMatchingDictIds();
    ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[dictIds.length];
    for (int i = 0; i < dictIds.length; i++) {
      bitmaps[i] = getDocIds(dictIds[i]);
    }
    return new BitmapCollection(_numDocs, _exclusive, bitmaps).excludingNulls(getNullBitmap());
  }

  /// Returns the documents for the given dictionary id, limited to `[0, numDocs)`.
  ///
  /// On a consuming segment, rows keep being indexed after the query is planned, so the bitmap read at execution time
  /// can hold documents at or above `numDocs`, which is read at planning time. These documents must be dropped: the
  /// flip for exclusive predicates only covers `[0, numDocs)`, so they would stay in the result, and the AND and count
  /// paths don't check the document range again.
  ///
  /// This applies the same limit as `FilterOperatorUtils.limitToNumDocs()`, which always copies the bitmap it trims.
  /// Here, a [MutableRoaringBitmap] is trimmed in place instead, see below.
  private ImmutableRoaringBitmap getDocIds(int dictId) {
    ImmutableRoaringBitmap docIds = _invertedIndexReader.getDocIds(dictId);
    if (docIds.isEmpty() || docIds.last() < _numDocs) {
      return docIds;
    }
    // A MutableRoaringBitmap from the reader is taken as a copy this operator may modify, as the in-place flip in
    // getNextBlockWithoutNullHandling() already does. The mutable inverted index, whose bitmaps hold the documents to
    // drop, returns a fresh clone on every call (RealtimeInvertedIndex.getDocIds() through
    // ThreadSafeMutableRoaringBitmap.getMutableRoaringBitmap()), so it is trimmed without another copy. Any other
    // bitmap is copied first.
    MutableRoaringBitmap mutableDocIds =
        docIds instanceof MutableRoaringBitmap ? (MutableRoaringBitmap) docIds : docIds.toMutableRoaringBitmap();
    mutableDocIds.remove(_numDocs, 0x100000000L);
    return mutableDocIds;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public List<Operator> getChildOperators() {
    return List.of();
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(indexLookUp:inverted_index");
    Predicate predicate = _predicateEvaluator.getPredicate();
    stringBuilder.append(",operator:").append(predicate.getType());
    stringBuilder.append(",predicate:").append(predicate);
    return stringBuilder.append(')').toString();
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putString("indexLookUp", "inverted_index");
    attributeBuilder.putString("operator", _predicateEvaluator.getPredicate().getType().name());
    attributeBuilder.putString("predicate", _predicateEvaluator.getPredicate().toString());
  }
}
