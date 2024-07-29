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
package org.apache.pinot.segment.local.segment.index.readers.vector;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * A simple collector created to bypass all the heap heavy process
 * of collecting the results in Lucene. Lucene by default will
 * create a {@link org.apache.lucene.search.TopScoreDocCollector}
 * which internally uses a {@link org.apache.lucene.search.TopDocsCollector}
 * and uses a PriorityQueue to maintain the top results. From the heap usage
 * experiments (please see the design doc), we found out that this was
 * substantially contributing to heap whereas we currently don't need any
 * scoring or top doc collecting.
 * Every time Lucene finds a matching document for the text search query,
 * a callback is invoked into this collector that simply collects the
 * matching doc's docID. We store the docID in a bitmap to be traversed later
 * as part of doc id iteration etc.
 */
public class HnswDocIdCollector implements Collector {

  private final MutableRoaringBitmap _docIds;
  private final HnswVectorIndexReader.DocIdTranslator _docIdTranslator;

  public HnswDocIdCollector(MutableRoaringBitmap docIds, HnswVectorIndexReader.DocIdTranslator docIdTranslator) {
    _docIds = docIds;
    _docIdTranslator = docIdTranslator;
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) {
    return new LeafCollector() {

      @Override
      public void setScorer(Scorable scorer)
          throws IOException {
        // we don't use scoring, so this is NO-OP
      }

      @Override
      public void collect(int doc)
          throws IOException {
        // Compute the absolute lucene docID across
        // sub-indexes because that's how the lookup table in docIdTranslator is built
        _docIds.add(_docIdTranslator.getPinotDocId(context.docBase + doc));
      }
    };
  }
}
