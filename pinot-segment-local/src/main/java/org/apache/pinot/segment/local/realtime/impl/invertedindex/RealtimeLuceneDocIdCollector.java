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
package org.apache.pinot.segment.local.realtime.impl.invertedindex;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * DocID collector for Lucene search query. We have optimized
 * the lucene search on offline segments by maintaining
 * a pre-built luceneDocId -> pinotDocId mapping. Since that solution
 * is not directly applicable to realtime, we will separate the collector
 * for the time-being. Once we have optimized the realtime, we can
 */
public class RealtimeLuceneDocIdCollector implements Collector {
  private volatile boolean _shouldCancel;
  private final MutableRoaringBitmap _docIds;

  public RealtimeLuceneDocIdCollector(MutableRoaringBitmap docIds) {
    _docIds = docIds;
    _shouldCancel = false;
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) {
    return new LeafCollector() {
      // Counter for periodic termination check
      private int _numDocsCollected = 0;

      @Override
      public void setScorer(Scorable scorer)
          throws IOException {
        // we don't use scoring, so this is NO-OP
      }

      @Override
      public void collect(int doc)
          throws IOException {
        if (_shouldCancel) {
          throw new RuntimeException("TEXT_MATCH query was cancelled");
        }
        try {
          QueryThreadContext.checkTerminationAndSampleUsagePeriodically(
              _numDocsCollected++, "RealtimeLuceneDocIdCollector");
        } catch (RuntimeException e) {
          // CollectionTerminatedException - Lucene's IndexSearcher.search() specially handles this exception
          // to gracefully stop document collection without treating it as an error.
          // When checkTerminationAndSampleUsagePeriodically() throws
          // TerminationException (for OOM/timeout), it's already stored in QueryExecutionContext._terminateException
          // before being thrown. After search completes, higher-level code retrieves the actual error via
          // QueryThreadContext.getTerminateException() to include proper error details in query response.
          throw new CollectionTerminatedException();
        }

        // Compute the absolute lucene docID across sub-indexes as doc that is passed is relative to the current reader
        _docIds.add(context.docBase + doc);
      }
    };
  }

  public void markShouldCancel() {
    _shouldCancel = true;
  }

  public MutableRoaringBitmap getDocIds() {
    return _docIds;
  }
}
