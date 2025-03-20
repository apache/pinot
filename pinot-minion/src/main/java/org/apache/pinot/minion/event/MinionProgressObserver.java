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
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.tasks.MinionTaskBaseObserverStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A minion event observer that can track task progress status in memory.
 */
@ThreadSafe
public class MinionProgressObserver extends DefaultMinionEventObserver {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinionProgressObserver.class);

  protected final MinionTaskBaseObserverStats _taskProgressStats = new MinionTaskBaseObserverStats();
  protected String _taskId;

  @Override
  public synchronized void notifyTaskStart(PinotTaskConfig pinotTaskConfig) {
    _taskProgressStats.setStartTimestamp(System.currentTimeMillis());
    _taskProgressStats.setCurrentState(MinionTaskState.IN_PROGRESS.name());
    _taskId = pinotTaskConfig.getTaskId();
    _taskProgressStats.setTaskId(_taskId);
    addStatus(new MinionTaskBaseObserverStats.StatusEntry.Builder()
        .withTs(_taskProgressStats.getStartTimestamp())
        .withStatus("Task started")
        .withStage(MinionTaskState.IN_PROGRESS.name())
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
    String progressMessage = null;
    MinionTaskBaseObserverStats.StatusEntry statusEntry = null;
    _taskProgressStats.setCurrentState(MinionTaskState.IN_PROGRESS.name());
    if (progress instanceof MinionTaskBaseObserverStats.StatusEntry) {
      statusEntry = (MinionTaskBaseObserverStats.StatusEntry) progress;
      progressMessage = statusEntry.getStatus();
    } else if (progress instanceof MinionTaskBaseObserverStats) {
      MinionTaskBaseObserverStats stats = (MinionTaskBaseObserverStats) progress;
      // Only one progress log must be recorded at once and should not be bulked
      if (stats.getProgressLogs() != null) {
        statusEntry = stats.getProgressLogs().pollFirst();
        progressMessage = statusEntry != null ? statusEntry.getStatus() : null;
      }
    } else if (progress != null) {
      progressMessage = progress.toString();
      statusEntry = new MinionTaskBaseObserverStats.StatusEntry.Builder()
          .withStatus(progressMessage)
          .build();
    }
    if (LOGGER.isDebugEnabled() && progressMessage != null) {
      LOGGER.debug("Update progress: {} for task: {}", progressMessage, pinotTaskConfig.getTaskId());
    }
    addStatus(statusEntry);
    super.notifyProgress(pinotTaskConfig, progress);
  }

  @Override
  @Nullable
  public synchronized List<MinionTaskBaseObserverStats.StatusEntry> getProgress() {
    MinionTaskBaseObserverStats minionTaskObserverStats = _observerStorageManager.getTaskProgress(_taskId);
    if (minionTaskObserverStats != null && minionTaskObserverStats.getProgressLogs() != null) {
      return new ArrayList<>(minionTaskObserverStats.getProgressLogs());
    }
    return null;
  }

  @Nullable
  @Override
  public MinionTaskBaseObserverStats getProgressStats() {
    return _observerStorageManager.getTaskProgress(_taskId);
  }

  @Override
  public synchronized void notifyTaskSuccess(PinotTaskConfig pinotTaskConfig, @Nullable Object executionResult) {
    long endTs = System.currentTimeMillis();
    _taskProgressStats.setCurrentState(MinionTaskState.SUCCEEDED.name());
    _taskProgressStats.setEndTimestamp(endTs);
    addStatus(new MinionTaskBaseObserverStats.StatusEntry.Builder()
        .withTs(endTs)
        .withStatus("Task succeeded in " + (endTs - _taskProgressStats.getStartTimestamp()) + "ms")
        .withStage(MinionTaskState.SUCCEEDED.name())
        .build());
    super.notifyTaskSuccess(pinotTaskConfig, executionResult);
  }

  @Override
  public synchronized void notifyTaskCancelled(PinotTaskConfig pinotTaskConfig) {
    long endTs = System.currentTimeMillis();
    _taskProgressStats.setCurrentState(MinionTaskState.CANCELLED.name());
    _taskProgressStats.setEndTimestamp(endTs);
    addStatus(new MinionTaskBaseObserverStats.StatusEntry.Builder()
        .withTs(endTs)
        .withLevel(MinionTaskBaseObserverStats.StatusEntry.LogLevel.WARN)
        .withStatus("Task got cancelled after " + (endTs - _taskProgressStats.getStartTimestamp()) + "ms")
        .withStage(MinionTaskState.CANCELLED.name())
        .build());
    super.notifyTaskCancelled(pinotTaskConfig);
  }

  @Override
  public synchronized void notifyTaskError(PinotTaskConfig pinotTaskConfig, Exception e) {
    long endTs = System.currentTimeMillis();
    _taskProgressStats.setCurrentState(MinionTaskState.ERROR.name());
    _taskProgressStats.setEndTimestamp(endTs);
    addStatus(new MinionTaskBaseObserverStats.StatusEntry.Builder()
        .withTs(endTs)
        .withLevel(MinionTaskBaseObserverStats.StatusEntry.LogLevel.ERROR)
        .withStatus("Task failed in " + (endTs - _taskProgressStats.getStartTimestamp()) + "ms with error: "
            + ExceptionUtils.getStackTrace(e))
        .withStage(MinionTaskState.ERROR.name())
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

  private synchronized void addStatus(MinionTaskBaseObserverStats.StatusEntry statusEntry) {
    if (statusEntry == null) {
      // if no status entry provided, only update the task progress if the _taskProgressStats has updated values
      MinionTaskBaseObserverStats minionTaskObserverStats = _observerStorageManager.getTaskProgress(_taskId);
      if (minionTaskObserverStats != null && !minionTaskObserverStats.equals(_taskProgressStats)) {
        _observerStorageManager.setTaskProgress(_taskId, new MinionTaskBaseObserverStats(_taskProgressStats));
      }
      return;
    }
    String incomingStage = statusEntry.getStage();
    if (_taskProgressStats.getCurrentStage() == null) {
      // typically incomingStage won't be null when current stage is also null as notifyTaskStart is the first
      // that gets called during task execution.
      // This handling is mostly for testing purpose
      _taskProgressStats.setCurrentStage(incomingStage != null ? incomingStage : MinionTaskState.UNKNOWN.name());
    }
    String currentStage = _taskProgressStats.getCurrentStage();
    Map<String, MinionTaskBaseObserverStats.Timer> stageTimes = _taskProgressStats.getStageTimes();
    if (incomingStage != null && !currentStage.equals(incomingStage)) {
      // stage transition
      stageTimes.get(currentStage).stop();
      currentStage = incomingStage;
      _taskProgressStats.setCurrentStage(currentStage);
    } else {
      // carry forward current stage if stage not specified
      statusEntry.updateStage(currentStage);
    }
    if (!stageTimes.containsKey(currentStage)) {
      stageTimes.put(currentStage, new MinionTaskBaseObserverStats.Timer());
      stageTimes.get(currentStage).start();
    }

    MinionTaskBaseObserverStats minionTaskObserverStats = _observerStorageManager.getTaskProgress(_taskId);
    Deque<MinionTaskBaseObserverStats.StatusEntry> progressLogs;
    if (minionTaskObserverStats != null) {
      progressLogs = minionTaskObserverStats.getProgressLogs();
    } else {
      progressLogs = new LinkedList<>();
    }
    progressLogs.offer(statusEntry);
    _observerStorageManager.setTaskProgress(_taskId, new MinionTaskBaseObserverStats(_taskProgressStats)
        .setProgressLogs(progressLogs));
  }

  @Override
  public void cleanup() {
    if (_taskId != null) {
      _observerStorageManager.deleteTaskProgress(_taskId);
    }
  }
}
