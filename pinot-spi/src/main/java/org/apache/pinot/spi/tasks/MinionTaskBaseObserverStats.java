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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Wrapper class to maintain and handle all the stats emitted by the various minion task executors.
 * Currently, its being used to maintain the stats of a task execution at MinionProgressObserver.
 * Eventually the MinionProgressObserver should also accept object of MinionTaskBaseObserverStats to record progress
 *
 */
public class MinionTaskBaseObserverStats {
  protected String _taskId;
  protected String _currentState;
  protected long _startTimestamp;
  protected Deque<StatusEntry> _progressLogs = new LinkedList<>();

  public MinionTaskBaseObserverStats() {
  }

  public MinionTaskBaseObserverStats(MinionTaskBaseObserverStats from) {
    _taskId = from.getTaskId();
    _currentState = from.getCurrentState();
    _startTimestamp = from.getStartTimestamp();
    _progressLogs = new LinkedList<>(from.getProgressLogs());
  }

  public String getTaskId() {
    return _taskId;
  }

  public MinionTaskBaseObserverStats setTaskId(String taskId) {
    _taskId = taskId;
    return this;
  }

  public long getStartTimestamp() {
    return _startTimestamp;
  }

  public MinionTaskBaseObserverStats setStartTimestamp(long startTimestamp) {
    _startTimestamp = startTimestamp;
    return this;
  }

  public String getCurrentState() {
    return _currentState;
  }

  public MinionTaskBaseObserverStats setCurrentState(String currentState) {
    _currentState = currentState;
    return this;
  }

  public Deque<StatusEntry> getProgressLogs() {
    return _progressLogs;
  }

  public MinionTaskBaseObserverStats setProgressLogs(Deque<StatusEntry> progressLogs) {
    _progressLogs = progressLogs;
    return this;
  }

  public MinionTaskBaseObserverStats fromJsonString(String statsJson)
      throws JsonProcessingException {
    return JsonUtils.stringToObject(statsJson, MinionTaskBaseObserverStats.class);
  }

  public String toJsonString()
      throws JsonProcessingException {
    return JsonUtils.objectToString(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MinionTaskBaseObserverStats stats = (MinionTaskBaseObserverStats) o;
    return _startTimestamp == stats.getStartTimestamp() && _taskId.equals(stats.getTaskId())
        && _currentState.equals(stats.getCurrentState());
  }

  @Override
  public int hashCode() {
    return Objects.hash(_taskId, _currentState, _startTimestamp);
  }

  @JsonDeserialize(builder = StatusEntry.Builder.class)
  public static class StatusEntry {
    private final long _ts;
    private final LogLevel _level;
    private final String _status;

    public StatusEntry(long ts, LogLevel level, String status) {
      _ts = ts;
      _level = level != null ? level : LogLevel.INFO;
      _status = status;
    }

    public long getTs() {
      return _ts;
    }

    public String getStatus() {
      return _status;
    }

    public LogLevel getLevel() {
      return _level;
    }

    @Override
    public String toString() {
      return "{\"ts\" : " + _ts + ", \"level\" : \"" + _level + "\", \"status\" : \"" + _status + "\"}";
    }

    public static class Builder {
      private long _ts;
      private LogLevel _level = LogLevel.INFO;
      private String _status;

      public Builder withTs(long ts) {
        _ts = ts;
        return this;
      }

      public Builder withLevel(LogLevel level) {
        _level = level;
        return this;
      }

      public Builder withStatus(String status) {
        _status = status;
        return this;
      }

      public StatusEntry build() {
        if (_ts == 0) {
          _ts = System.currentTimeMillis();
        }
        return new StatusEntry(_ts, _level, _status);
      }
    }

    public enum LogLevel {
      INFO, WARN, ERROR
    }
  }
}
