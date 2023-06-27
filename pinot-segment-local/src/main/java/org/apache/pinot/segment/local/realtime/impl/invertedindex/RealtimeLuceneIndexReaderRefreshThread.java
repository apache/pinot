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

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import org.apache.lucene.search.SearcherManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Background thread to refresh the realtime lucene index readers for supporting
 * near-realtime text search. The task maintains a queue of realtime segments.
 * This queue is global (across all realtime segments of all realtime/hybrid tables).
 *
 * Each element in the queue is of type {@link RealtimeLuceneIndexRefreshState.RealtimeLuceneReaders}.
 * It encapsulates a lock and all the realtime lucene readers for the particular realtime segment.
 * Since text index is also create on a per column basis, there will be as many realtime lucene
 * readers as the number of columns with text search enabled.
 *
 * Between each successive execution of the task, there is a fixed delay (regardless of how long
 * each execution took). When the task wakes up, it pick the RealtimeLuceneReadersForRealtimeSegment
 * from the head of queue, refresh it's readers and adds this at the tail of queue.
 */
public class RealtimeLuceneIndexReaderRefreshThread implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeLuceneIndexReaderRefreshThread.class);
  // TODO: make this configurable and choose a higher default value
  private static final int DELAY_BETWEEN_SUCCESSIVE_EXECUTION_MS_DEFAULT = 10;

  private final ConcurrentLinkedQueue<RealtimeLuceneIndexRefreshState.RealtimeLuceneReaders> _luceneRealtimeReaders;
  private final Lock _mutex;
  private final Condition _conditionVariable;

  private volatile boolean _stopped = false;

  RealtimeLuceneIndexReaderRefreshThread(
      ConcurrentLinkedQueue<RealtimeLuceneIndexRefreshState.RealtimeLuceneReaders> luceneRealtimeReaders, Lock mutex,
      Condition conditionVariable) {
    _luceneRealtimeReaders = luceneRealtimeReaders;
    _mutex = mutex;
    _conditionVariable = conditionVariable;
  }

  void setStopped() {
    _stopped = true;
  }

  @Override
  public void run() {
    while (!_stopped) {
      _mutex.lock();
      try {
        // During instantiation of a given MutableSegmentImpl, we will signal on this condition variable once
        // one or more realtime lucene readers (one per column) belonging to the MutableSegment
        // are added to the global queue managed by this thread. The thread that signals will
        // grab this mutex and signal on the condition variable.
        //
        // This refresh thread will be woken up (and grab the mutex automatically as per the
        // implementation of await) and check if the queue is non-empty. It will then proceed to
        // poll the queue and refresh the realtime index readers for the polled segment.
        //
        // The mutex and condition-variable semantics take care of the scenario when on
        // a given Pinot server, there is no realtime segment with text index enabled. In such
        // cases, there is no need for this thread to wake up simply after every few seconds/minutes
        // only to find that there is nothing to be refreshed. The thread should simply be
        // off CPU until signalled specifically. This also covers the situation where initially
        // there were few realtime segments of a table with text index. Later if they got
        // moved to another server as part of rebalance, then again there is no need for this thread
        // to do anything until some realtime segment is created with text index enabled.
        while (_luceneRealtimeReaders.isEmpty()) {
          _conditionVariable.await();
        }
      } catch (InterruptedException e) {
        LOGGER.warn("Realtime lucene reader refresh thread got interrupted while waiting on condition variable: ", e);
        Thread.currentThread().interrupt();
      } finally {
        _mutex.unlock();
      }

      // check if shutdown has been initiated
      if (_stopped) {
        // exit
        break;
      }

      // remove the realtime segment from the front of queue
      RealtimeLuceneIndexRefreshState.RealtimeLuceneReaders realtimeReadersForSegment = _luceneRealtimeReaders.poll();
      if (realtimeReadersForSegment != null) {
        String segmentName = realtimeReadersForSegment.getSegmentName();
        // take the lock to prevent the realtime segment from being concurrently destroyed
        // and thus closing the realtime readers while this thread attempts to refresh them
        realtimeReadersForSegment.getLock().lock();
        try {
          if (!realtimeReadersForSegment.isSegmentDestroyed()) {
            // if the segment hasn't yet been destroyed, refresh each
            // realtime reader (one per column with text index enabled)
            // for this segment.
            List<RealtimeLuceneTextIndex> realtimeLuceneReaders =
                realtimeReadersForSegment.getRealtimeLuceneReaders();
            for (RealtimeLuceneTextIndex realtimeReader : realtimeLuceneReaders) {
              if (_stopped) {
                // exit
                break;
              }
              SearcherManager searcherManager = realtimeReader.getSearcherManager();
              try {
                searcherManager.maybeRefresh();
              } catch (Exception e) {
                // we should never be here since the locking semantics between MutableSegmentImpl::destroy()
                // and this code along with volatile state "isSegmentDestroyed" protect against the cases
                // where this thread might attempt to refresh a realtime lucene reader after it has already
                // been closed duing segment destroy.
                LOGGER.warn("Caught exception {} while refreshing realtime lucene reader for segment: {}", e,
                    segmentName);
              }
            }
          }
        } finally {
          if (!realtimeReadersForSegment.isSegmentDestroyed()) {
            _luceneRealtimeReaders.offer(realtimeReadersForSegment);
          }
          realtimeReadersForSegment.getLock().unlock();
        }
      }

      try {
        Thread.sleep(DELAY_BETWEEN_SUCCESSIVE_EXECUTION_MS_DEFAULT);
      } catch (Exception e) {
        LOGGER.warn("Realtime lucene reader refresh thread got interrupted while sleeping: ", e);
        Thread.currentThread().interrupt();
      }
    } // end while
  }
}
