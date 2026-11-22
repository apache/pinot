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
import java.util.concurrent.CancellationException;
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


/// Collects every matching document together with its relevance score.
///
/// Unlike [LuceneDocIdCollector], this collector asks Lucene for scores ([ScoreMode#COMPLETE]), so the
/// searcher's similarity (BM25 by default) is evaluated for each match. It deliberately does not keep a top-k
/// priority queue: Pinot applies its own ORDER BY / LIMIT after projection, and every document that survives the
/// filter needs a score, not only the top ones.
///
/// Immutable segments translate index-wide Lucene document IDs through the segment mapping. Consuming segments
/// read the stored Pinot document ID from the current leaf.
///
/// Not thread-safe: one instance serves one search.
public class LuceneDocScoreCollector implements Collector {
  private final Int2FloatMap _scores = new Int2FloatOpenHashMap();
  @Nullable
  private final IntUnaryOperator _docIdMapper;
  private volatile boolean _shouldCancel;

  public LuceneDocScoreCollector(@Nullable IntUnaryOperator docIdMapper) {
    _docIdMapper = docIdMapper;
  }

  public static LuceneDocScoreCollector forConsumingSegment() {
    return new LuceneDocScoreCollector(null);
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
