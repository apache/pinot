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
package org.apache.pinot.spi.tasks;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;


public class MinionTaskProgressStats {
  protected String _taskId;
  protected String _currentState;
  protected long _startTimestamp;
  protected Deque<StatusEntry> _progressLogs;

  public MinionTaskProgressStats() {
  }

  public MinionTaskProgressStats(MinionTaskProgressStats from) {
    _taskId = from.getTaskId();
    _currentState = from.getCurrentState();
    _startTimestamp = from.getStartTimestamp();
    _progressLogs = new LinkedList<>(from.getProgressLogs());
  }

  public String getTaskId() {
    return _taskId;
  }

  public MinionTaskProgressStats setTaskId(String taskId) {
    _taskId = taskId;
    return this;
  }

  public long getStartTimestamp() {
    return _startTimestamp;
  }

  public MinionTaskProgressStats setStartTimestamp(long startTimestamp) {
    _startTimestamp = startTimestamp;
    return this;
  }

  public String getCurrentState() {
    return _currentState;
  }

  public MinionTaskProgressStats setCurrentState(String currentState) {
    _currentState = currentState;
    return this;
  }

  public Deque<StatusEntry> getProgressLogs() {
    return _progressLogs;
  }

  public MinionTaskProgressStats setProgressLogs(Deque<StatusEntry> progressLogs) {
    _progressLogs = progressLogs;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MinionTaskProgressStats stats = (MinionTaskProgressStats) o;
    return _startTimestamp == stats._startTimestamp && _taskId.equals(stats._taskId)
        && _currentState.equals(stats._currentState);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_taskId, _currentState, _startTimestamp);
  }
}
