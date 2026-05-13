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
package org.apache.pinot.spi.query;

import java.util.concurrent.atomic.LongAdder;


/**
 * Lightweight, thread-safe accumulator for query scan cost metrics.
 *
 * <p>One instance is created per query when scan-based killing is enabled.
 * Attached to {@link QueryExecutionContext} and shared across all segment
 * worker threads executing that query.</p>
 *
 */
public final class QueryScanCostContext {
  private final LongAdder _numEntriesScannedInFilter = new LongAdder();
  private final LongAdder _numDocsScanned = new LongAdder();
  private final LongAdder _numEntriesScannedPostFilter = new LongAdder();
  private final long _startTimeMs = System.currentTimeMillis();

  /**
   * Increment entries scanned during filter evaluation.
   * Called from {@code DocIdSetOperator} after each block.
   */
  public void addEntriesScannedInFilter(long count) {
    _numEntriesScannedInFilter.add(count);
  }

  /**
   * Increment documents scanned (passed filter, processed by query operator).
   * Called from {@code AggregationOperator}, {@code SelectionOnlyOperator},
   * {@code GroupByOperator} after each value block.
   */
  public void addDocsScanned(long count) {
    _numDocsScanned.add(count);
  }

  /**
   * Increment entries scanned post-filter (docs * projected columns).
   * Called from query operators after projection.
   */
  public void addEntriesScannedPostFilter(long count) {
    _numEntriesScannedPostFilter.add(count);
  }

  public long getNumEntriesScannedInFilter() {
    return _numEntriesScannedInFilter.sum();
  }

  public long getNumDocsScanned() {
    return _numDocsScanned.sum();
  }

  public long getNumEntriesScannedPostFilter() {
    return _numEntriesScannedPostFilter.sum();
  }

  /**
   * Returns wall-clock time elapsed since this context was created.
   */
  public long getElapsedTimeMs() {
    return System.currentTimeMillis() - _startTimeMs;
  }
}
