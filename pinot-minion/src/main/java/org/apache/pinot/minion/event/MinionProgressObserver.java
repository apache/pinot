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

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.tasks.MinionTaskProgressStats;
import org.apache.pinot.spi.tasks.StatusEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A minion event observer that can track task progress status in memory.
 */
@ThreadSafe
public class MinionProgressObserver extends DefaultMinionEventObserver {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinionProgressObserver.class);

  protected MinionTaskProgressStats _taskProgressStats = new MinionTaskProgressStats();
  protected String _taskId;

  @Override
  public synchronized void notifyTaskStart(PinotTaskConfig pinotTaskConfig) {
    _taskProgressStats.setStartTimestamp(System.currentTimeMillis());
    _taskProgressStats.setCurrentState(MinionTaskState.IN_PROGRESS.name());
    _taskId = pinotTaskConfig.getTaskId();
    _taskProgressStats.setTaskId(_taskId);
    addStatus(new StatusEntry.Builder()
        .timestamp(_taskProgressStats.getStartTimestamp())
        .status("Task started")
        .build());
    super.notifyTaskStart(pinotTaskConfig);
  }

  /**
   * Invoked to update a minion task progress status.
   *
   * @param pinotTaskConfig Pinot task config
   * @param progress progress status and its toString() returns sth meaningful.
   */
  @Override
  public synchronized void notifyProgress(PinotTaskConfig pinotTaskConfig, @Nullable Object progress) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Update progress: {} for task: {}", progress, pinotTaskConfig.getTaskId());
    }
    _taskProgressStats.setCurrentState(MinionTaskState.IN_PROGRESS.name());
    addStatus(new StatusEntry.Builder()
        .timestamp(System.currentTimeMillis())
        .status((progress == null) ? "" : progress.toString())
        .build());
    super.notifyProgress(pinotTaskConfig, progress);
  }

  @Override
  @Nullable
  public synchronized List<StatusEntry> getProgress() {
    MinionTaskProgressStats minionTaskProgressStats = _progressManager.getTaskProgress(_taskId);
    if (minionTaskProgressStats != null && minionTaskProgressStats.getProgressLogs() != null) {
      return new ArrayList<>(minionTaskProgressStats.getProgressLogs());
    }
    return null;
  }

  @Nullable
  @Override
  public MinionTaskProgressStats getProgressStats() {
    return _progressManager.getTaskProgress(_taskId);
  }

  @Override
  public synchronized void notifyTaskSuccess(PinotTaskConfig pinotTaskConfig, @Nullable Object executionResult) {
    long endTs = System.currentTimeMillis();
    _taskProgressStats.setCurrentState(MinionTaskState.SUCCEEDED.name());
    addStatus(new StatusEntry.Builder()
        .timestamp(endTs)
        .status("Task succeeded in " + (endTs - _taskProgressStats.getStartTimestamp()) + "ms")
        .build());
    super.notifyTaskSuccess(pinotTaskConfig, executionResult);
  }

  @Override
  public synchronized void notifyTaskCancelled(PinotTaskConfig pinotTaskConfig) {
    long endTs = System.currentTimeMillis();
    _taskProgressStats.setCurrentState(MinionTaskState.CANCELLED.name());
    addStatus(new StatusEntry.Builder()
        .timestamp(endTs)
        .status("Task got cancelled after " + (endTs - _taskProgressStats.getStartTimestamp()) + "ms")
        .build());
    super.notifyTaskCancelled(pinotTaskConfig);
  }

  @Override
  public synchronized void notifyTaskError(PinotTaskConfig pinotTaskConfig, Exception e) {
    long endTs = System.currentTimeMillis();
    _taskProgressStats.setCurrentState(MinionTaskState.ERROR.name());
    addStatus(new StatusEntry.Builder()
        .timestamp(endTs)
        .status("Task failed in " + (endTs - _taskProgressStats.getStartTimestamp()) + "ms with error: "
            + ExceptionUtils.getStackTrace(e))
        .build());
    super.notifyTaskError(pinotTaskConfig, e);
  }

  @Override
  public MinionTaskState getTaskState() {
    if (_taskProgressStats.getCurrentState() != null) {
      return MinionTaskState.valueOf(_taskProgressStats.getCurrentState());
    }
    return MinionTaskState.UNKNOWN;
  }

  @Override
  public long getStartTs() {
    return _taskProgressStats.getStartTimestamp();
  }

  private synchronized void addStatus(StatusEntry statusEntry) {
    MinionTaskProgressStats minionTaskProgressStats = _progressManager.getTaskProgress(_taskId);
    Deque<StatusEntry> progressLogs;
    if (minionTaskProgressStats != null) {
      progressLogs = minionTaskProgressStats.getProgressLogs();
    } else {
      progressLogs = new LinkedList<>();
    }
    progressLogs.add(statusEntry);

    _progressManager.setTaskProgress(_taskId, new MinionTaskProgressStats(_taskProgressStats)
        .setProgressLogs(progressLogs));
  }

  @Override
  public void cleanup() {
    if (_taskId != null) {
      _progressManager.deleteTaskProgress(_taskId);
    }
  }
}
