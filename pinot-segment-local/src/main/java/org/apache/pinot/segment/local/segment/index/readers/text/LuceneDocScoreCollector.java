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
package org.apache.pinot.segment.local.segment.index.readers.text;

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.function.IntPredicate;
import java.util.function.IntUnaryOperator;
import javax.annotation.Nullable;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils.StoredPinotDocIdReader;
import org.apache.pinot.spi.query.QueryThreadContext;


/// Collects matching documents together with their relevance scores, optionally filtered to a subset of Pinot doc IDs.
///
/// Unlike [LuceneDocIdCollector], this collector asks Lucene for scores ([ScoreMode#COMPLETE]), so the
/// searcher's similarity (BM25 by default) is evaluated for each collected match. When an allowed Pinot doc ID
/// predicate is supplied, documents outside the predicate are skipped before invoking [Scorable#score()] or inserting
/// into the score map, bounding heap usage to the allowed set.
///
/// Immutable segments translate index-wide Lucene document IDs through the segment mapping. Consuming segments
/// read the stored Pinot document ID from the current leaf.
///
/// Not thread-safe: one instance serves one search.
public class LuceneDocScoreCollector implements Collector {
  private final Int2FloatMap _scores = new Int2FloatOpenHashMap();
  @Nullable
  private final IntUnaryOperator _docIdMapper;
  @Nullable
  private final IntPredicate _allowedPinotDocIds;
  private volatile boolean _shouldCancel;

  public LuceneDocScoreCollector(@Nullable IntUnaryOperator docIdMapper) {
    this(docIdMapper, null);
  }

  public LuceneDocScoreCollector(@Nullable IntUnaryOperator docIdMapper, @Nullable IntPredicate allowedPinotDocIds) {
    _docIdMapper = docIdMapper;
    _allowedPinotDocIds = allowedPinotDocIds;
  }

  public static LuceneDocScoreCollector forConsumingSegment() {
    return new LuceneDocScoreCollector(null, null);
  }

  public static LuceneDocScoreCollector forConsumingSegment(@Nullable IntPredicate allowedPinotDocIds) {
    return new LuceneDocScoreCollector(null, allowedPinotDocIds);
  }

  /// Builds an [IntPredicate] testing membership in `docIds[0 .. length - 1]` with an O(1) `[min, max]` range guard.
  ///
  /// When `docIds` is already sorted ascending, the returned predicate aliases `docIds` directly without copying, so
  /// callers must not mutate or recycle `docIds` while the predicate is in use. Use
  /// [#allowedDocIdsPredicate(int[], int, boolean)] with `copyIfShared = true` when the predicate may execute
  /// asynchronously on another thread.
  public static IntPredicate allowedDocIdsPredicate(int[] docIds, int length) {
    return allowedDocIdsPredicate(docIds, length, false);
  }

  /// Builds an [IntPredicate] testing membership in `docIds[0 .. length - 1]` with an O(1) `[min, max]` range guard.
  /// Reduces single-element and contiguous ascending ranges to pure primitive bounds checks with zero array retention.
  ///
  /// @param copyIfShared when `true`, copies a non-contiguous sorted `docIds` array so asynchronous consumers (such as
  ///                     the realtime searcher pool) never race with a cancelled/timed-out caller reusing a
  ///                     thread-local doc-id buffer
  public static IntPredicate allowedDocIdsPredicate(int[] docIds, int length, boolean copyIfShared) {
    if (length <= 0) {
      return id -> false;
    }
    if (length == 1) {
      int target = docIds[0];
      return id -> id == target;
    }
    boolean strictlyAscending = true;
    boolean nonDecreasing = true;
    for (int i = 1; i < length; i++) {
      if (docIds[i] < docIds[i - 1]) {
        nonDecreasing = false;
        strictlyAscending = false;
        break;
      } else if (docIds[i] == docIds[i - 1]) {
        strictlyAscending = false;
      }
    }
    if (strictlyAscending && (long) docIds[length - 1] - docIds[0] == length - 1L) {
      int minDocId = docIds[0];
      int maxDocId = docIds[length - 1];
      return id -> id >= minDocId && id <= maxDocId;
    }
    int[] targets;
    if (nonDecreasing) {
      targets = copyIfShared ? Arrays.copyOf(docIds, length) : docIds;
    } else {
      targets = Arrays.copyOf(docIds, length);
      Arrays.sort(targets);
    }
    int minDocId = targets[0];
    int maxDocId = targets[length - 1];
    return id -> id >= minDocId && id <= maxDocId && Arrays.binarySearch(targets, 0, length, id) >= 0;
  }

  /// Lucene 9's single-collector API keeps one score map on one thread, with one cancellation flag for the search.
  @SuppressWarnings("deprecation")
  public Int2FloatMap collectScores(IndexSearcher searcher, Query query)
      throws IOException {
    searcher.search(query, this);
    return _scores;
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context)
      throws IOException {
    StoredPinotDocIdReader storedDocIds =
        _docIdMapper == null ? new StoredPinotDocIdReader(context.reader().storedFields()) : null;
    return new LeafCollector() {
      private Scorable _scorer;
      private int _numDocsCollected = 0;

      @Override
      public void setScorer(Scorable scorer) {
        _scorer = scorer;
      }

      @Override
      public void collect(int doc)
          throws IOException {
        if (_shouldCancel) {
          throw new CancellationException("TEXT_SCORE query was cancelled");
        }
        QueryThreadContext.checkTerminationAndSampleUsagePeriodically(_numDocsCollected++, "LuceneDocScoreCollector");
        int pinotDocId = storedDocIds != null ? storedDocIds.getPinotDocId(doc)
            : _docIdMapper.applyAsInt(context.docBase + doc);
        if (_allowedPinotDocIds != null && !_allowedPinotDocIds.test(pinotDocId)) {
          return;
        }
        _scores.put(pinotDocId, _scorer.score());
      }
    };
  }

  /// Stops the search at the next collected document, for a query thread that stopped waiting for it (see the
  /// realtime text indexes, which search on a separate pool).
  public void markShouldCancel() {
    _shouldCancel = true;
  }
}
