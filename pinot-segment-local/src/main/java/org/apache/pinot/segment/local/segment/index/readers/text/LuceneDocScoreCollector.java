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
import java.io.IOException;
import java.util.function.IntUnaryOperator;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.pinot.spi.query.QueryThreadContext;


/// Collects every matching document together with its relevance score.
///
/// Unlike [LuceneDocIdCollector], this collector asks Lucene for scores ([ScoreMode#COMPLETE]), so the
/// searcher's similarity (BM25 by default) is evaluated for each match. It deliberately does not keep a top-k
/// priority queue: Pinot applies its own ORDER BY / LIMIT after projection, and every document that survives the
/// filter needs a score, not only the top ones.
///
/// Lucene doc ids are translated with the supplied mapper before they are stored. The mapper receives the
/// index-wide Lucene doc id (`docBase + doc`), matching how [DocIdTranslator] is built.
///
/// Not thread-safe: one instance serves one search.
public class LuceneDocScoreCollector implements Collector {
  private final Int2FloatMap _scores;
  private final IntUnaryOperator _docIdMapper;

  public LuceneDocScoreCollector(Int2FloatMap scores, IntUnaryOperator docIdMapper) {
    _scores = scores;
    _docIdMapper = docIdMapper;
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) {
    int docBase = context.docBase;
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
        try {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(_numDocsCollected++,
              "LuceneDocScoreCollector");
        } catch (RuntimeException e) {
          // See LuceneDocIdCollector: the termination cause is already recorded on the query context, and
          // CollectionTerminatedException makes Lucene stop collecting without treating it as an error.
          throw new CollectionTerminatedException();
        }
        _scores.put(_docIdMapper.applyAsInt(docBase + doc), _scorer.score());
      }
    };
  }
}
