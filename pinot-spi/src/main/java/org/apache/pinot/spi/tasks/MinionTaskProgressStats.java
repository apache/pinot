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

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MinionTaskProgressStats {
  protected String _taskId;
  protected String _currentStage;
  protected String _currentState;
  protected Map<String, Timer> _stageTimes = new HashMap<>();
  protected long _startTimestamp;
  protected long _endTimestamp;
  protected List<Map<String, String>> _inputUnits;
  protected int _inputUnitsProcessed;
  protected int _segmentsGenerated;
  protected List<StatusEntry> _progressLogs;

  public String getTaskId() {
    return _taskId;
  }

  public MinionTaskProgressStats setTaskId(String taskId) {
    _taskId = taskId;
    return this;
  }

  public long getEndTimestamp() {
    return _endTimestamp;
  }

  public MinionTaskProgressStats setEndTimestamp(long endTimestamp) {
    _endTimestamp = endTimestamp;
    return this;
  }

  public long getStartTimestamp() {
    return _startTimestamp;
  }

  public MinionTaskProgressStats setStartTimestamp(long startTimestamp) {
    _startTimestamp = startTimestamp;
    return this;
  }

  public Map<String, Timer> getStageTimes() {
    return _stageTimes;
  }

  public MinionTaskProgressStats setStageTimes(Map<String, Timer> stageTimes) {
    _stageTimes = stageTimes;
    return this;
  }

  public String getCurrentState() {
    return _currentState;
  }

  public MinionTaskProgressStats setCurrentState(String currentState) {
    _currentState = currentState;
    return this;
  }

  public String getCurrentStage() {
    return _currentStage;
  }

  public MinionTaskProgressStats setCurrentStage(String currentStage) {
    _currentStage = currentStage;
    return this;
  }

  public int getSegmentsGenerated() {
    return _segmentsGenerated;
  }

  public MinionTaskProgressStats setSegmentsGenerated(int segmentsGenerated) {
    _segmentsGenerated = segmentsGenerated;
    return this;
  }

  public List<Map<String, String>> getInputUnits() {
    return _inputUnits;
  }

  public MinionTaskProgressStats setInputUnits(List<Map<String, String>> inputUnits) {
    _inputUnits = inputUnits;
    return this;
  }

  public int getInputUnitsProcessed() {
    return _inputUnitsProcessed;
  }

  public MinionTaskProgressStats setInputUnitsProcessed(int inputUnitsProcessed) {
    _inputUnitsProcessed = inputUnitsProcessed;
    return this;
  }

  public List<StatusEntry> getProgressLogs() {
    return _progressLogs;
  }

  public MinionTaskProgressStats setProgressLogs(List<StatusEntry> progressLogs) {
    _progressLogs = progressLogs;
    return this;
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
}
