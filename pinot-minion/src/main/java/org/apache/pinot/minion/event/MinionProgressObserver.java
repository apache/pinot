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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.tasks.MinionTaskProgressStats;
import org.apache.pinot.spi.tasks.StatusEntry;


/**
 * A minion event observer that can track task progress status in memory.
 */
@ThreadSafe
public class MinionProgressObserver extends DefaultMinionEventObserver {

  protected MinionTaskState _taskState;
  protected final Map<String, MinionTaskProgressStats.Timer> _stageTimes = new HashMap<>();
  protected String _stage;
  protected long _startTs;
  protected long _endTs;
  protected List<Map<String, String>> _inputUnits;
  protected int _inputUnitsProcessed;
  protected int _segmentsGenerated;
  protected List<StatusEntry> _progressBuffer = new ArrayList<>();
  protected String _taskId;

  @Override
  public synchronized void notifyTaskStart(PinotTaskConfig pinotTaskConfig) {
    _startTs = System.currentTimeMillis();
    _taskState = MinionTaskState.IN_PROGRESS;
    _taskId = pinotTaskConfig.getTaskId();
    setStageStats(new StatusEntry.Builder()
        .timestamp(_startTs)
        .stage(_taskState.name())
        .status("Task started")
        .build());
  }

  @Override
  public synchronized void notifyProgress(PinotTaskConfig pinotTaskConfig, @Nullable Object progress) {
    _taskState = MinionTaskState.IN_PROGRESS;
    if (progress instanceof StatusEntry) {
      setStageStats((StatusEntry) progress);
    } else if (progress instanceof MinionTaskProgressStats) {
      MinionTaskProgressStats stats = (MinionTaskProgressStats) progress;
      if (stats.getInputUnits() != null && !stats.getInputUnits().isEmpty()) {
        _inputUnits = stats.getInputUnits();
      }
      if (stats.getSegmentsGenerated() > 0) {
        _segmentsGenerated = stats.getSegmentsGenerated();
      }
      if (stats.getInputUnitsProcessed() > 0) {
        _inputUnitsProcessed = stats.getInputUnitsProcessed();
      }
      // Only one progress log must be recorded at once and should not be bulked
      if (stats.getProgressLogs() != null && stats.getProgressLogs().size() == 1) {
        setStageStats(stats.getProgressLogs().get(0));
      }
    } else {
      String progressMessage = progress == null ? "" : progress.toString();
      setStageStats(new StatusEntry.Builder().status(progressMessage).build());
    }
  }

  @Nullable
  @Override
  public synchronized List<StatusEntry> getProgress() {
    MinionTaskProgressStats minionTaskProgressStats = _progressManager.getTaskProgress(_taskId);
    List<StatusEntry> progressLog = new ArrayList<>();
    if (minionTaskProgressStats != null) {
      progressLog.addAll(minionTaskProgressStats.getProgressLogs());
    }
    progressLog.addAll(_progressBuffer);
    return progressLog;
  }

  @Nullable
  @Override
  public MinionTaskProgressStats getProgressStats() {
    return buildProgressStats();
  }

  @Override
  public synchronized void notifyTaskSuccess(PinotTaskConfig pinotTaskConfig, @Nullable Object executionResult) {
    _endTs = System.currentTimeMillis();
    if (executionResult instanceof List) {
      List<Object> results = (List<Object>) executionResult;
      _segmentsGenerated = results.size();
    }
    _taskState = MinionTaskState.SUCCEEDED;
    setStageStats(new StatusEntry.Builder()
        .timestamp(_endTs)
        .stage(_taskState.name())
        .status("Task succeeded in " + (_endTs - _startTs) + "ms")
        .build());
  }

  @Override
  public synchronized void notifyTaskCancelled(PinotTaskConfig pinotTaskConfig) {
    _endTs = System.currentTimeMillis();
    _taskState = MinionTaskState.CANCELLED;
    setStageStats(new StatusEntry.Builder()
        .timestamp(_endTs)
        .stage(_taskState.name())
        .level(StatusEntry.LogLevel.WARN)
        .status("Task got cancelled after " + (_endTs - _startTs) + "ms")
        .build());
  }

  @Override
  public synchronized void notifyTaskError(PinotTaskConfig pinotTaskConfig, Exception e) {
    _endTs = System.currentTimeMillis();
    _taskState = MinionTaskState.ERROR;
    setStageStats(new StatusEntry.Builder()
        .timestamp(_endTs)
        .stage(_taskState.name())
        .level(StatusEntry.LogLevel.ERROR)
        .status("Task failed in " + (_endTs - _startTs) + "ms with error: " + ExceptionUtils.getStackTrace(e))
        .build());
  }

  @Override
  public MinionTaskState getTaskState() {
    return _taskState;
  }

  @Override
  public long getStartTs() {
    return _startTs;
  }

  protected MinionTaskProgressStats buildProgressStats() {
    MinionTaskProgressStats minionTaskProgressStats = _progressManager.getTaskProgress(_taskId);
    if (minionTaskProgressStats == null) {
      minionTaskProgressStats = new MinionTaskProgressStats();
      minionTaskProgressStats.setProgressLogs(new ArrayList<>(_progressBuffer));
    } else {
      minionTaskProgressStats.getProgressLogs().addAll(_progressBuffer);
    }
    return minionTaskProgressStats
        .setTaskId(_taskId)
        .setCurrentStage(_stage)
        .setCurrentState(_taskState.name())
        .setStageTimes(_stageTimes)
        .setStartTimestamp(_startTs)
        .setEndTimestamp(_endTs)
        .setInputUnits(_inputUnits)
        .setInputUnitsProcessed(_inputUnitsProcessed)
        .setSegmentsGenerated(_segmentsGenerated);
  }

  protected synchronized void setStageStats(StatusEntry progress) {
    String incomingStage = progress.getStage();
    if (_stage == null) {
      _stage = incomingStage != null ? incomingStage : MinionTaskState.UNKNOWN.name();
    }
    if (incomingStage != null && !_stage.equals(incomingStage)) {
      _stageTimes.get(_stage).stop();
      _stage = incomingStage;
    }
    if (!_stageTimes.containsKey(_stage)) {
      _stageTimes.put(_stage, new MinionTaskProgressStats.Timer());
    }
    if (_endTs == 0) {
      _stageTimes.get(_stage).start();
    }
    _progressBuffer.add(progress);
    if (_progressBuffer.size() >= _progressManager.getProgressBufferSize() || _endTs > 0) {
      flush();
    }
  }

  public synchronized void flush() {
    _progressManager.setTaskProgress(_taskId, buildProgressStats());
    _progressBuffer.clear();
  }

  @Override
  public void cleanup() {
    if (_taskId != null) {
      _progressManager.deleteTaskProgress(_taskId);
    }
  }
}
