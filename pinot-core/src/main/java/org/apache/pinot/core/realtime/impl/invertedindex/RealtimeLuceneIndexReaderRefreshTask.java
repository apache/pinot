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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.search.SearcherManager;


public class RealtimeLuceneIndexReaderRefreshTask implements Runnable {
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, RealtimeLuceneIndexReader>> _luceneRealtimeReaders;

  public RealtimeLuceneIndexReaderRefreshTask(ConcurrentHashMap<String, ConcurrentHashMap<String, RealtimeLuceneIndexReader>> luceneRealtimeReaders) {
    _luceneRealtimeReaders = luceneRealtimeReaders;
  }

  @Override
  public void run() {
    Iterator segmentIterator = _luceneRealtimeReaders.keySet().iterator();
    while (segmentIterator.hasNext()) {
      String segmentName = (String)segmentIterator.next();
      Iterator<Map.Entry<String, RealtimeLuceneIndexReader>> segmentIndexIterator = _luceneRealtimeReaders.get(segmentName).entrySet().iterator();
      while (segmentIndexIterator.hasNext()) {
        Map.Entry<String, RealtimeLuceneIndexReader> entry = segmentIndexIterator.next();
        RealtimeLuceneIndexReader realtimeLuceneIndexReader = entry.getValue();
        SearcherManager searcherManager = realtimeLuceneIndexReader.getSearcherManager();
        try {
          searcherManager.maybeRefresh();
        } catch (Exception e) {
          // we should swallow this since when trying to refresh a realtime reader,
          // the refresh() call will see an exception if the writer has been closed.
          // or if the searcherManager has been closed.
          // this will happen when MutableSegmentImpl is destroyed and we cleanup
          // the resources
          // TODO: see if this can be handled better
        }
      }
    }
  }
}