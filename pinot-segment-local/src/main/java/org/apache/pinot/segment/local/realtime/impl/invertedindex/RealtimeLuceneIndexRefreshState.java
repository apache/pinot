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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * This class manages the realtime lucene index readers. Creates a global
 * queue with all the realtime segment lucene index readers across
 * all tables and manages their refresh using {@link RealtimeLuceneIndexReaderRefreshThread}
 *
 * TODO: eventually we should explore partitioning this queue on per table basis
 */
public class RealtimeLuceneIndexRefreshState {
  private static RealtimeLuceneIndexRefreshState _singletonInstance;
  private static RealtimeLuceneIndexReaderRefreshThread _realtimeRefreshThread;
  private final Lock _mutex;
  private final Condition _conditionVariable;
  private static ConcurrentLinkedQueue<RealtimeLuceneReaders> _luceneRealtimeReaders;

  private RealtimeLuceneIndexRefreshState() {
    _mutex = new ReentrantLock();
    _conditionVariable = _mutex.newCondition();
    _luceneRealtimeReaders = new ConcurrentLinkedQueue<>();
  }

  /**
   * Used by HelixServerStarter during bootstrap to create the singleton
   * instance of this class and start the realtime reader refresh thread.
   */
  public void start() {
    _realtimeRefreshThread =
        new RealtimeLuceneIndexReaderRefreshThread(_luceneRealtimeReaders, _mutex, _conditionVariable);
    Thread t = new Thread(_realtimeRefreshThread);
    t.start();
  }

  /**
   * Used by HelixServerStarter during shutdown. This sets the volatile
   * "stopped" variable to indicate the shutdown to refresh thread.
   * Since refresh thread might be suspended waiting on the condition variable,
   * we signal the condition variable for the refresh thread to wake up,
   * check that shutdown has been initiated and exit.
   */
  public void stop() {
    _realtimeRefreshThread.setStopped();
    _mutex.lock();
    _conditionVariable.signal();
    _mutex.unlock();
  }

  public static RealtimeLuceneIndexRefreshState getInstance() {
    if (_singletonInstance == null) {
      synchronized (RealtimeLuceneIndexRefreshState.class) {
        if (_singletonInstance == null) {
          _singletonInstance = new RealtimeLuceneIndexRefreshState();
        }
      }
    }
    return _singletonInstance;
  }

  public void addRealtimeReadersToQueue(RealtimeLuceneReaders readersForRealtimeSegment) {
    _mutex.lock();
    _luceneRealtimeReaders.offer(readersForRealtimeSegment);
    _conditionVariable.signal();
    _mutex.unlock();
  }

  /**
   * Since the text index is maintained per TEXT column (similar to other Pinot indexes),
   * there could be multiple lucene indexes for a given segment and therefore there can be
   * multiple realtime lucene readers (one for each index/column) for the particular
   * realtime segment.
   */
  public static class RealtimeLuceneReaders {
    private final String segmentName;
    private final Lock lock;
    private boolean segmentDestroyed;
    private final List<RealtimeLuceneTextIndexReader> realtimeLuceneReaders;

    public RealtimeLuceneReaders(String segmentName) {
      this.segmentName = segmentName;
      lock = new ReentrantLock();
      segmentDestroyed = false;
      realtimeLuceneReaders = new LinkedList<>();
    }

    public void addReader(RealtimeLuceneTextIndexReader realtimeLuceneTextIndexReader) {
      realtimeLuceneReaders.add(realtimeLuceneTextIndexReader);
    }

    public void setSegmentDestroyed() {
      segmentDestroyed = true;
    }

    public Lock getLock() {
      return lock;
    }

    public String getSegmentName() {
      return segmentName;
    }

    public List<RealtimeLuceneTextIndexReader> getRealtimeLuceneReaders() {
      return realtimeLuceneReaders;
    }

    public void clearRealtimeReaderList() {
      realtimeLuceneReaders.clear();
    }

    boolean isSegmentDestroyed() {
      return segmentDestroyed;
    }
  }
}
