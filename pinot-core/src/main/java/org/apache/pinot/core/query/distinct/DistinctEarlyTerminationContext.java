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
package org.apache.pinot.core.query.distinct;

import java.util.function.LongSupplier;

/**
 * Tracks per-block early-termination budgets for distinct executors (row limits and no-change limits).
 * <p>All distinct executors should delegate to this class so that query options such as
 * {@code maxRowsInDistinct} and {@code numRowsWithoutChangeInDistinct} are enforced consistently
 * while processing each {@link org.apache.pinot.core.operator.blocks.ValueBlock}.</p>
 */
public class DistinctEarlyTerminationContext {
  private static final int UNLIMITED_ROWS = Integer.MAX_VALUE;
  private static final long UNLIMITED_TIME_NANOS = Long.MAX_VALUE;

  private int _rowsRemaining = UNLIMITED_ROWS;
  private int _numRowsProcessed = 0;
  private int _numRowsWithoutChangeLimit = UNLIMITED_ROWS;
  private int _numRowsWithoutChange = 0;
  private boolean _numRowsWithoutChangeLimitReached = false;
  // Absolute deadline (in nanos from the configured time supplier). Using a deadline instead of a fixed remaining
  // value allows executors to stop within a block as time elapses.
  private long _deadlineTimeNanos = UNLIMITED_TIME_NANOS;
  private LongSupplier _timeSupplier = System::nanoTime;

  public void setTimeSupplier(LongSupplier timeSupplier) {
    if (timeSupplier == null || timeSupplier == _timeSupplier) {
      return;
    }
    if (_deadlineTimeNanos != UNLIMITED_TIME_NANOS) {
      // Preserve the already computed remaining budget when switching time sources (primarily for tests).
      long remainingTimeNanos = getRemainingTimeNanos();
      _timeSupplier = timeSupplier;
      setRemainingTimeNanos(remainingTimeNanos);
    } else {
      _timeSupplier = timeSupplier;
    }
  }

  public void setMaxRowsToProcess(int maxRows) {
    _rowsRemaining = maxRows;
  }

  public int getRemainingRowsToProcess() {
    return _rowsRemaining;
  }

  public void setNumRowsWithoutChangeInDistinct(int numRowsWithoutChangeInDistinct) {
    _numRowsWithoutChangeLimit = numRowsWithoutChangeInDistinct;
  }

  public boolean isNumRowsWithoutChangeLimitReached() {
    return _numRowsWithoutChangeLimitReached;
  }

  public int getNumRowsProcessed() {
    return _numRowsProcessed;
  }

  public int clampToRemaining(int numDocs) {
    if (_rowsRemaining == UNLIMITED_ROWS) {
      return numDocs;
    }
    if (_rowsRemaining <= 0) {
      return 0;
    }
    return Math.min(numDocs, _rowsRemaining);
  }

  public void recordRowProcessed(boolean distinctChanged) {
    _numRowsProcessed++;
    if (_rowsRemaining != UNLIMITED_ROWS) {
      _rowsRemaining--;
    }
    if (_numRowsWithoutChangeLimit != UNLIMITED_ROWS) {
      if (distinctChanged) {
        _numRowsWithoutChange = 0;
      } else {
        _numRowsWithoutChange++;
        if (_numRowsWithoutChange >= _numRowsWithoutChangeLimit) {
          _numRowsWithoutChangeLimitReached = true;
        }
      }
    }
  }

  public boolean shouldStopProcessing() {
    return _rowsRemaining <= 0 || _numRowsWithoutChangeLimitReached || getRemainingTimeNanos() <= 0;
  }

  public long getRemainingTimeNanos() {
    if (_deadlineTimeNanos == UNLIMITED_TIME_NANOS) {
      return UNLIMITED_TIME_NANOS;
    }
    return _deadlineTimeNanos - _timeSupplier.getAsLong();
  }

  public void setRemainingTimeNanos(long remainingTimeNanos) {
    if (remainingTimeNanos == UNLIMITED_TIME_NANOS) {
      _deadlineTimeNanos = UNLIMITED_TIME_NANOS;
      return;
    }
    long now = _timeSupplier.getAsLong();
    if (remainingTimeNanos <= 0) {
      _deadlineTimeNanos = now;
      return;
    }
    try {
      _deadlineTimeNanos = Math.addExact(now, remainingTimeNanos);
    } catch (ArithmeticException e) {
      // Saturate to "unlimited" if the deadline overflows.
      _deadlineTimeNanos = UNLIMITED_TIME_NANOS;
    }
  }
}
