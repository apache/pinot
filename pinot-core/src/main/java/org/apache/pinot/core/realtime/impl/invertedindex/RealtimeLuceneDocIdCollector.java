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
package org.apache.pinot.core.realtime.impl.invertedindex;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * DocID collector for Lucene search query. We have optimized
 * the lucene search on offline segments by maintaining
 * a pre-built luceneDocId -> pinotDocId mapping. Since that solution
 * is not directly applicable to realtime, we will separate the collector
 * for the time-being. Once we have optimized the realtime, we can
 */
public class RealtimeLuceneDocIdCollector implements Collector {

  private final MutableRoaringBitmap _docIds;

  public RealtimeLuceneDocIdCollector(MutableRoaringBitmap docIds) {
    _docIds = docIds;
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) {
    return new LeafCollector() {

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        // we don't use scoring, so this is NO-OP
      }

      @Override
      public void collect(int doc) throws IOException {
        _docIds.add(doc);
      }
    };
  }
}
