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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
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
  protected String _currentStage;
  protected String _currentState;
  protected long _startTimestamp;
  protected long _endTimestamp;
  protected Map<String, Timer> _stageTimes = new HashMap<>();
  protected Deque<StatusEntry> _progressLogs = new LinkedList<>();

  public MinionTaskBaseObserverStats() {
  }

  public MinionTaskBaseObserverStats(MinionTaskBaseObserverStats from) {
    _taskId = from.getTaskId();
    _currentState = from.getCurrentState();
    _currentStage = from.getCurrentStage();
    _startTimestamp = from.getStartTimestamp();
    _endTimestamp = from.getEndTimestamp();
    _stageTimes = from.getStageTimes();
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

  public long getEndTimestamp() {
    return _endTimestamp;
  }

  public MinionTaskBaseObserverStats setEndTimestamp(long endTimestamp) {
    _endTimestamp = endTimestamp;
    return this;
  }

  public String getCurrentState() {
    return _currentState;
  }

  public MinionTaskBaseObserverStats setCurrentState(String currentState) {
    _currentState = currentState;
    return this;
  }

  public String getCurrentStage() {
    return _currentStage;
  }

  public MinionTaskBaseObserverStats setCurrentStage(String currentStage) {
    _currentStage = currentStage;
    return this;
  }

  public Map<String, Timer> getStageTimes() {
    return _stageTimes;
  }

  public MinionTaskBaseObserverStats setStageTimes(Map<String, Timer> stageTimes) {
    _stageTimes = stageTimes;
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
    return _startTimestamp == stats.getStartTimestamp() && _endTimestamp == stats.getEndTimestamp()
        && _taskId.equals(stats.getTaskId()) && _currentState.equals(stats.getCurrentState())
        && _currentStage.equals(stats.getCurrentStage());
  }

  @Override
  public int hashCode() {
    return Objects.hash(_taskId, _currentStage, _currentState, _startTimestamp, _endTimestamp);
  }

  public static class Timer {
    private long _totalTimeMs = 0;
    private long _startTimeMs = 0;

    public void start() {
      _startTimeMs = System.currentTimeMillis();
    }

    public void stop() {
      if (_startTimeMs != 0) {
        _totalTimeMs += System.currentTimeMillis() - _startTimeMs;
        _startTimeMs = 0;
      }
    }
    public long getTotalTimeMs() {
      return _totalTimeMs;
    }
  }

  @JsonDeserialize(builder = StatusEntry.Builder.class)
  public static class StatusEntry {
    private final long _ts;
    private final LogLevel _level;
    private final String _status;
    private String _stage;

    private StatusEntry(long ts, LogLevel level, String status, String stage) {
      _ts = ts;
      _level = level != null ? level : LogLevel.INFO;
      _status = status;
      _stage = stage;
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

    public String getStage() {
      return _stage;
    }

    public boolean updateStage(String stage) {
      if (_stage == null) {
        _stage = stage;
        return true;
      }
      return false;
    }

    @Override
    public String toString() {
      return "{\"ts\" : " + _ts + ", \"level\" : \"" + _level + "\", \"stage\" : \"" + _stage
          + "\", \"status\" : \"" + _status + "\"}";
    }

    public static class Builder {
      private long _ts;
      private LogLevel _level = LogLevel.INFO;
      private String _stage;
      private String _status;

      public Builder withTs(long ts) {
        _ts = ts;
        return this;
      }

      public Builder withLevel(LogLevel level) {
        _level = level;
        return this;
      }

      public Builder withStage(String stage) {
        _stage = stage;
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
        return new StatusEntry(_ts, _level, _status, _stage);
      }
    }

    public enum LogLevel {
      INFO, WARN, ERROR
    }
  }
}
