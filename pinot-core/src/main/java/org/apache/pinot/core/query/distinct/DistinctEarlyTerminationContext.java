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

import com.google.common.annotations.VisibleForTesting;
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
  private boolean _maxRowsLimitReached = false;
  private boolean _trackingEnabled = false;
  // Absolute deadline (in nanos from the configured time supplier). A deadline stays consistent with the time source
  // and enables budget checks.
  private long _deadlineTimeNanos = UNLIMITED_TIME_NANOS;
  private boolean _timeLimitReached = false;
  private LongSupplier _timeSupplier = System::nanoTime;

  @VisibleForTesting
  public void setTimeSupplier(LongSupplier timeSupplier) {
    if (timeSupplier == null || timeSupplier == _timeSupplier) {
      return;
    }
    _timeSupplier = timeSupplier;
  }

  public void setMaxRowsToProcess(int maxRows) {
    _rowsRemaining = maxRows;
    if (maxRows != UNLIMITED_ROWS) {
      _trackingEnabled = true;
      if (maxRows <= 0) {
        _maxRowsLimitReached = true;
      }
    }
  }

  public int getRemainingRowsToProcess() {
    if (!_trackingEnabled) {
      return UNLIMITED_ROWS;
    }
    return _rowsRemaining;
  }

  public boolean isTrackingEnabled() {
    return _trackingEnabled;
  }

  public boolean isDistinctChangeTrackingEnabled() {
    return _numRowsWithoutChangeLimit != UNLIMITED_ROWS;
  }

  public void setNumRowsWithoutChangeInDistinct(int numRowsWithoutChangeInDistinct) {
    _numRowsWithoutChangeLimit = numRowsWithoutChangeInDistinct;
    if (numRowsWithoutChangeInDistinct != UNLIMITED_ROWS) {
      _trackingEnabled = true;
    }
  }

  public boolean isNumRowsWithoutChangeLimitReached() {
    return _numRowsWithoutChangeLimitReached;
  }

  public int getNumRowsProcessed() {
    if (!_trackingEnabled) {
      return 0;
    }
    return _numRowsProcessed;
  }

  public int clampToRemaining(int numDocs) {
    if (!_trackingEnabled || _rowsRemaining == UNLIMITED_ROWS) {
      return numDocs;
    }
    if (_rowsRemaining <= 0) {
      return 0;
    }
    return Math.min(numDocs, _rowsRemaining);
  }

  public void recordRowProcessed(boolean distinctChanged) {
    if (!_trackingEnabled) {
      return;
    }
    _numRowsProcessed++;
    if (_rowsRemaining != UNLIMITED_ROWS) {
      _rowsRemaining--;
      if (_rowsRemaining <= 0) {
        _maxRowsLimitReached = true;
      }
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

  public boolean shouldStopProcessingWithoutTime() {
    if (!_trackingEnabled) {
      return false;
    }
    if (_rowsRemaining <= 0) {
      _maxRowsLimitReached = true;
    }
    return _rowsRemaining <= 0 || _numRowsWithoutChangeLimitReached;
  }

  public boolean shouldStopProcessing() {
    if (!_trackingEnabled) {
      return false;
    }
    return shouldStopProcessingWithoutTime() || getRemainingTimeNanos() <= 0;
  }

  public long getRemainingTimeNanos() {
    if (_deadlineTimeNanos == UNLIMITED_TIME_NANOS) {
      return UNLIMITED_TIME_NANOS;
    }
    long remaining = _deadlineTimeNanos - _timeSupplier.getAsLong();
    if (remaining <= 0) {
      _timeLimitReached = true;
    }
    return remaining;
  }

  /**
   * Sets the remaining time budget in nanoseconds. If the computed deadline overflows, the budget is treated as
   * unlimited to avoid spurious early termination from a wrapped deadline.
   */
  public void setRemainingTimeNanos(long remainingTimeNanos) {
    if (remainingTimeNanos == UNLIMITED_TIME_NANOS) {
      _deadlineTimeNanos = UNLIMITED_TIME_NANOS;
      return;
    }
    _trackingEnabled = true;
    long now = _timeSupplier.getAsLong();
    if (remainingTimeNanos <= 0) {
      _deadlineTimeNanos = now;
      _timeLimitReached = true;
      return;
    }
    try {
      _deadlineTimeNanos = Math.addExact(now, remainingTimeNanos);
    } catch (ArithmeticException e) {
      // Saturate to "unlimited" if the computed deadline overflows.
      _deadlineTimeNanos = UNLIMITED_TIME_NANOS;
    }
  }

  public boolean isMaxRowsLimitReached() {
    return _maxRowsLimitReached;
  }

  public boolean isTimeLimitReached() {
    if (_deadlineTimeNanos == UNLIMITED_TIME_NANOS) {
      return false;
    }
    if (_timeLimitReached) {
      return true;
    }
    // Update based on current time budget.
    getRemainingTimeNanos();
    return _timeLimitReached;
  }
}
