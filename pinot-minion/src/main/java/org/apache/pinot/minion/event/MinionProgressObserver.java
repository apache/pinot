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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A minion event observer that can track task progress status in memory.
 */
@ThreadSafe
public class MinionProgressObserver extends DefaultMinionEventObserver {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinionProgressObserver.class);
  // TODO: make this configurable
  private static final int DEFAULT_MAX_NUM_STATUS_TO_TRACK = 128;

  private final int _maxNumStatusToTrack;
  private final Deque<StatusEntry> _lastStatus = new LinkedList<>();
  private MinionTaskState _taskState;
  private long _startTs;

  public MinionProgressObserver() {
    this(DEFAULT_MAX_NUM_STATUS_TO_TRACK);
  }

  public MinionProgressObserver(int maxNumStatusToTrack) {
    _maxNumStatusToTrack = maxNumStatusToTrack;
    _taskState = MinionTaskState.UNKNOWN;
  }

  @Override
  public synchronized void notifyTaskStart(PinotTaskConfig pinotTaskConfig) {
    _startTs = System.currentTimeMillis();
    addStatus(_startTs, "Task started", MinionTaskState.IN_PROGRESS);
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
    addStatus(System.currentTimeMillis(), (progress == null) ? "" : progress.toString(), MinionTaskState.IN_PROGRESS);
    super.notifyProgress(pinotTaskConfig, progress);
  }

  @Override
  @Nullable
  public synchronized List<StatusEntry> getProgress() {
    return new ArrayList<>(_lastStatus);
  }

  @Override
  public synchronized void notifyTaskSuccess(PinotTaskConfig pinotTaskConfig, @Nullable Object executionResult) {
    long endTs = System.currentTimeMillis();
    addStatus(endTs, "Task succeeded in " + (endTs - _startTs) + "ms", MinionTaskState.SUCCEEDED);
    super.notifyTaskSuccess(pinotTaskConfig, executionResult);
  }

  @Override
  public synchronized void notifyTaskCancelled(PinotTaskConfig pinotTaskConfig) {
    long endTs = System.currentTimeMillis();
    addStatus(endTs, "Task got cancelled after " + (endTs - _startTs) + "ms", MinionTaskState.CANCELLED);
    super.notifyTaskCancelled(pinotTaskConfig);
  }

  @Override
  public synchronized void notifyTaskError(PinotTaskConfig pinotTaskConfig, Exception e) {
    long endTs = System.currentTimeMillis();
    addStatus(endTs, "Task failed in " + (endTs - _startTs) + "ms with error: " + ExceptionUtils.getStackTrace(e),
        MinionTaskState.ERROR);
    super.notifyTaskError(pinotTaskConfig, e);
  }

  @Override
  public MinionTaskState getTaskState() {
    return _taskState;
  }

  @Override
  public long getStartTs() {
    return _startTs;
  }

  private void addStatus(long ts, String progress, MinionTaskState taskState) {
    _taskState = taskState;
    _lastStatus.addLast(new StatusEntry(ts, progress));
    if (_lastStatus.size() > _maxNumStatusToTrack) {
      _lastStatus.pollFirst();
    }
  }

  public static class StatusEntry {
    private final long _ts;
    private final String _status;

    public StatusEntry(long ts, String status) {
      _ts = ts;
      _status = status;
    }

    public long getTs() {
      return _ts;
    }

    public String getStatus() {
      return _status;
    }

    @Override
    public String toString() {
      return "StatusEntry{" + "_ts=" + _ts + ", _status=" + _status + '}';
    }
  }
}
