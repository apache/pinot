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
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.distinct.table.DistinctTable;


/**
 * Interface class for executing the distinct queries.
 */
public interface DistinctExecutor {
  // TODO: Tune the initial capacity
  int MAX_INITIAL_CAPACITY = 10000;

  /**
   * Sets the maximum number of rows to process across all blocks. Implementations should respect this limit and avoid
   * reading more rows once exhausted. Default implementation is a no-op for executors that do not support it.
   */
  default void setMaxRowsToProcess(int maxRows) {
  }

  /**
   * Returns the remaining number of rows that can be processed. Implementations that do not support early termination
   * should return {@link Integer#MAX_VALUE}.
   */
  default int getRemainingRowsToProcess() {
    return Integer.MAX_VALUE;
  }

  /**
   * Sets the maximum number of rows to scan without adding any new distinct value before early-terminating.
   */
  default void setNumRowsWithoutChangeInDistinct(int numRowsWithoutChangeInDistinct) {
  }

  /**
   * Sets the time supplier used by the executor to evaluate time-based early termination. Implementations should call
   * {@link LongSupplier#getAsLong()} in nanoseconds and treat it as a monotonic clock (e.g. {@link System#nanoTime()}).
   */
  default void setTimeSupplier(LongSupplier timeSupplier) {
  }

  /**
   * Updates the remaining time budget in nanoseconds. Implementations may use this to early terminate work within a
   * block when the time budget is exhausted.
   */
  default void setRemainingTimeNanos(long remainingTimeNanos) {
  }

  /**
   * Returns {@code true} if the executor has early-terminated because no new distinct values were found after scanning
   * the configured number of rows.
   */
  default boolean isNumRowsWithoutChangeLimitReached() {
    return false;
  }

  /**
   * Returns {@code true} if the executor has early-terminated because the maximum row budget was exhausted.
   */
  default boolean isMaxRowsLimitReached() {
    return false;
  }

  /**
   * Returns {@code true} if the executor has early-terminated because the time budget was exhausted.
   */
  default boolean isTimeLimitReached() {
    return false;
  }

  /**
   * Returns the total number of rows processed so far.
   */
  default int getNumRowsProcessed() {
    return 0;
  }

  /**
   * Processes the given value block, returns {@code true} if the query is already satisfied, {@code false}
   * otherwise. No more calls should be made after it returns {@code true}.
   */
  boolean process(ValueBlock valueBlock);

  /**
   * Returns the distinct result. Note that the returned DistinctTable might not be a main DistinctTable, thus cannot be
   * used to merge other records or tables, but can only be merged into the main DistinctTable.
   */
  DistinctTable getResult();

  /**
   * Returns the number of distinct rows collected so far.
   */
  int getNumDistinctRowsCollected();
}
