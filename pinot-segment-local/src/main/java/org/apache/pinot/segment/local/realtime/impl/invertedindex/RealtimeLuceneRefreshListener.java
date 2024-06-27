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

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.function.Supplier;
import org.apache.lucene.search.ReferenceManager;


/**
 * RealtimeLuceneRefreshListener is a listener that listens to the refresh of a Lucene index reader
 * and updates the metrics for the delay between the last refresh time and the current time, as well as
 * the number of documents that have been added since the last refresh.
 * <p>
 * RefreshListener.beforeRefresh() is called before a refresh attempt, and RefreshListener.afterRefresh() is called
 * after the refresh attempt. If the lock cannot be acquired for a refresh, then neither method will be called.
 * <p>
 * Tracking can be used in the future to handoff between a small live index, and the current reference searcher.
 */
public class RealtimeLuceneRefreshListener implements ReferenceManager.RefreshListener {

  private final RealtimeLuceneIndexingDelayTracker _realtimeLuceneIndexingDelayTracker;
  private final int _partition;
  private final String _tableName;
  private final String _segmentName;
  private final String _columnName;
  private final Supplier<Integer> _numDocsSupplier;
  private final Supplier<Integer> _numDocsDelaySupplier;
  private final Supplier<Long> _timeMsDelaySupplier;
  private long _lastRefreshTimeMs;
  private int _lastRefreshNumDocs;
  private int _numDocsBeforeRefresh;
  private Clock _clock;

  /**
   * Create a new RealtimeLuceneRefreshListener with a clock.
   * @param tableName Table name
   * @param segmentName Segment name
   * @param partition Partition number
   * @param numDocsSupplier Supplier for retrieving the current of documents in the index
   */
  public RealtimeLuceneRefreshListener(String tableName, String segmentName, String columnName, int partition,
      Supplier<Integer> numDocsSupplier) {
    this(tableName, segmentName, columnName, partition, numDocsSupplier, Clock.systemUTC());
  }

  /**
   * Create a new RealtimeLuceneRefreshListener with a clock. Intended for testing.
   * @param tableName Table name
   * @param segmentName Segment name
   * @param partition Partition number
   * @param numDocsSupplier Supplier for retrieving the current of documents in the index
   * @param clock Clock to use for time
   */
  @VisibleForTesting
  public RealtimeLuceneRefreshListener(String tableName, String segmentName, String columnName, int partition,
      Supplier<Integer> numDocsSupplier, Clock clock) {
    _partition = partition;
    _tableName = tableName;
    _segmentName = segmentName;
    _columnName = columnName;
    _numDocsSupplier = numDocsSupplier;
    _clock = clock;

    // When the listener is first created, the reader is current
    _lastRefreshTimeMs = _clock.millis();
    _lastRefreshNumDocs = _numDocsSupplier.get();

    // Initialize delay suppliers
    _numDocsDelaySupplier = initNumDocsDelaySupplier();
    _timeMsDelaySupplier = initTimeMsDelaySupplier();

    // Register with RealtimeLuceneIndexingDelayTracker
    _realtimeLuceneIndexingDelayTracker = RealtimeLuceneIndexingDelayTracker.getInstance();
    _realtimeLuceneIndexingDelayTracker.registerDelaySuppliers(_tableName, _segmentName, _columnName, _partition,
        _numDocsDelaySupplier, _timeMsDelaySupplier);
  }

  @Override
  public void beforeRefresh() {
    // Record the lower bound of the number of docs that a refreshed searcher might see
    _numDocsBeforeRefresh = _numDocsSupplier.get();
  }

  /**
   * @param didRefresh true if the searcher reference was swapped to a new reference, otherwise false
   */
  @Override
  public void afterRefresh(boolean didRefresh) {
    // Even if didRefresh is false, we should still update the last refresh time so that the delay is more accurate
    if (didRefresh || _lastRefreshNumDocs == _numDocsBeforeRefresh) {
      _lastRefreshTimeMs = _clock.millis();
      _lastRefreshNumDocs = _numDocsBeforeRefresh;
    }
  }

  private Supplier<Integer> initNumDocsDelaySupplier() {
    return () -> _numDocsSupplier.get() - _lastRefreshNumDocs;
  }

  private Supplier<Long> initTimeMsDelaySupplier() {
    return () -> {
      if (_numDocsDelaySupplier.get() == 0) {
        // if numDocsDelay is zero, consider the reference refreshed
        _lastRefreshTimeMs = _clock.millis();
      }
      return _clock.millis() - _lastRefreshTimeMs;
    };
  }

  public void close() {
    _realtimeLuceneIndexingDelayTracker.clear(_tableName, _segmentName, _columnName, _partition);
  }

  @VisibleForTesting
  public Supplier<Integer> getNumDocsDelaySupplier() {
    return _numDocsDelaySupplier;
  }

  @VisibleForTesting
  public Supplier<Long> getTimeMsDelaySupplier() {
    return _timeMsDelaySupplier;
  }

  @VisibleForTesting
  public void setClock(Clock clock) {
    _clock = clock;
  }

  @VisibleForTesting
  public Clock getClock() {
    return _clock;
  }
}
