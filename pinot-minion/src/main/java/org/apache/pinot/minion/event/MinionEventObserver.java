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
package org.apache.pinot.minion.event;

import javax.annotation.Nullable;
import org.apache.pinot.core.minion.PinotTaskConfig;


/**
 * The <code>MinionEventObserver</code> interface provides call backs for Minion events.
 */
public interface MinionEventObserver {

  /**
   * Invoked when a minion task starts.
   *
   * @param pinotTaskConfig Pinot task config
   */
  void notifyTaskStart(PinotTaskConfig pinotTaskConfig);

  /**
   * Invoked to update a minion task progress status.
   *
   * @param pinotTaskConfig Pinot task config
   * @param progress progress status
   */
  default void notifyProgress(PinotTaskConfig pinotTaskConfig, @Nullable Object progress) {
  }

  @Nullable
  default Object getProgress() {
    return null;
  }

  /**
   * Invoked when a minion task succeeds.
   *
   * @param pinotTaskConfig Pinot task config
   * @param executionResult Execution result
   */
  void notifyTaskSuccess(PinotTaskConfig pinotTaskConfig, @Nullable Object executionResult);

  /**
   * Invoked when a minion task gets cancelled.
   *
   * @param pinotTaskConfig Pinot task config
   */
  void notifyTaskCancelled(PinotTaskConfig pinotTaskConfig);

  /**
   * Invoked when a minion task encounters exception.
   *
   * @param pinotTaskConfig Pinot task config
   * @param exception Exception encountered during execution
   */
  void notifyTaskError(PinotTaskConfig pinotTaskConfig, Exception exception);

  /**
   * Gets the minion task state
   * @return a {@link MinionTaskState}
   */
  default MinionTaskState getTaskState() {
    return MinionTaskState.UNKNOWN;
  }

  /**
   * Gets the minion task start timestamp
   * @return the minion task start timestamp
   */
  default long getStartTs() {
    return -1;
  }
}
