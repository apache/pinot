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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.tasks.MinionTaskProgressManager;
import org.apache.pinot.spi.tasks.MinionTaskProgressStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A minion event observer that can track task progress status in memory.
 */
@ThreadSafe
public class MinionProgressObserver extends DefaultMinionEventObserver {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinionProgressObserver.class);

  protected MinionTaskState _taskState;
  protected final Map<String, MinionTaskProgressStats.Timer> _stageTimes = new HashMap<>();
  protected Set<String> _stages = new HashSet<>();
  protected String _stage;
  protected long _startTs;
  protected long _endTs;
  protected int _segmentsGenerated;
  protected List<MinionTaskProgressStats.StatusEntry> _progressBuffer = new ArrayList<>();
  protected MinionTaskProgressManager _progressManager;
  protected String _taskId;

  public MinionProgressObserver(MinionTaskProgressManager progressManager) {
    _progressManager = progressManager;
  }

  @Override
  public synchronized void notifyTaskStart(PinotTaskConfig pinotTaskConfig) {
    _startTs = System.currentTimeMillis();
    _taskState = MinionTaskState.IN_PROGRESS;
    _taskId = pinotTaskConfig.getTaskId();
    setStageStats(new MinionTaskProgressStats.StatusEntry(_startTs, _taskState.name(), "Task started"));
  }

  @Override
  public synchronized void notifyTaskSuccess(PinotTaskConfig pinotTaskConfig, @Nullable Object executionResult) {
    _endTs = System.currentTimeMillis();
    if (executionResult instanceof List) {
      List<Object> results = (List<Object>) executionResult;
      _segmentsGenerated = results.size();
    }
    _taskState = MinionTaskState.SUCCEEDED;
    setStageStats(new MinionTaskProgressStats.StatusEntry(_endTs, _taskState.name(), "Task succeeded in "
        + (_endTs - _startTs) + "ms"));
    flush();
  }

  @Override
  public synchronized void notifyTaskCancelled(PinotTaskConfig pinotTaskConfig) {
    _endTs = System.currentTimeMillis();
    _taskState = MinionTaskState.CANCELLED;
    setStageStats(new MinionTaskProgressStats.StatusEntry(_endTs, _taskState.name(),
        "Task got cancelled after " + (_endTs - _startTs) + "ms"));
    flush();
  }

  @Override
  public synchronized void notifyTaskError(PinotTaskConfig pinotTaskConfig, Exception e) {
    _endTs = System.currentTimeMillis();
    _taskState = MinionTaskState.ERROR;
    setStageStats(new MinionTaskProgressStats.StatusEntry(_endTs, _taskState.name(),
        "Task failed in " + (_endTs - _startTs) + "ms with error: " + ExceptionUtils.getStackTrace(e)));
    flush();
  }

  @Override
  public synchronized void notifyProgress(PinotTaskConfig pinotTaskConfig, @Nullable Object progress) {
    _taskState = MinionTaskState.IN_PROGRESS;
    if (progress instanceof MinionTaskProgressStats.StatusEntry) {
      setStageStats((MinionTaskProgressStats.StatusEntry) progress);
    } else {
      String progressMessage = progress == null ? "" : progress.toString();
      setStageStats(new MinionTaskProgressStats.StatusEntry(_stage, progressMessage));
    }
  }

  @Nullable
  @Override
  public synchronized List<MinionTaskProgressStats.StatusEntry> getProgress() {
    return getProgressStats().getProgressLogs();
  }

  @Override
  public MinionTaskState getTaskState() {
    return _taskState;
  }

  @Override
  public long getStartTs() {
    return _startTs;
  }

  public MinionTaskProgressStats getProgressStats() {
    MinionTaskProgressStats minionTaskProgressStats = _progressManager.getTaskProgress(_taskId);
    List<MinionTaskProgressStats.StatusEntry> progressLog = new ArrayList<>();
    if (minionTaskProgressStats != null) {
      progressLog.addAll(minionTaskProgressStats.getProgressLogs());
    }
    return buildProgressStats(progressLog);
  }

  private MinionTaskProgressStats buildProgressStats(List<MinionTaskProgressStats.StatusEntry> progressLog) {
    progressLog.addAll(_progressBuffer);
    return new MinionTaskProgressStats()
        .setTaskId(_taskId)
        .setCurrentStage(_stage)
        .setCurrentState(_taskState.name())
        .setStageTimes(_stageTimes)
        .setStartTimestamp(_startTs)
        .setEndTimestamp(_endTs)
        .setSegmentsGenerated(_segmentsGenerated)
        .setProgressLogs(progressLog);
  }

  protected void setStageStats(MinionTaskProgressStats.StatusEntry progress) {
    String incomingStage = progress.getStage();
    if (_stage != null && !_stage.equals(incomingStage)) {
      _stageTimes.get(_stage).stop();
    }
    if (_endTs != 0) {
      _stage = incomingStage;
    } else if (_stages.contains(incomingStage)) {
      _stage = incomingStage;
      _stageTimes.get(_stage).start();
    }
    _progressBuffer.add(progress);
    if (_progressBuffer.size() >= _progressManager.getProgressBufferSize()) {
      flush();
    }
  }

  public void flush() {
    _progressManager.setTaskProgress(_taskId, getProgressStats());
    _progressBuffer.clear();
  }

  public void setStages(Set<String> stages) {
    _stages = stages;
    stages.forEach(stage -> _stageTimes.put(stage, new MinionTaskProgressStats.Timer()));
  }
}
