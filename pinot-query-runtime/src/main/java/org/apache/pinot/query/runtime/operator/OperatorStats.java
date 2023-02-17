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
package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Stopwatch;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OperatorStats {
  private final Stopwatch _executeStopwatch = Stopwatch.createUnstarted();

  // TODO: add a operatorId for better tracking purpose.
  private final int _stageId;
  private final long _requestId;

  private final String _operatorType;

  private int _numBlock = 0;
  private int _numRows = 0;
  private Map<String, String> _executionStats;

  public OperatorStats(long requestId, int stageId, String operatorType) {
    _stageId = stageId;
    _requestId = requestId;
    _operatorType = operatorType;
    _executionStats = new HashMap<>();
  }

  public void startTimer() {
    if (!_executeStopwatch.isRunning()) {
      _executeStopwatch.start();
    }
  }

  public void endTimer() {
    if (_executeStopwatch.isRunning()) {
      _executeStopwatch.stop();
    }
  }

  public void recordRow(int numBlock, int numRows) {
    _numBlock += numBlock;
    _numRows += numRows;
  }

  public void recordExecutionStats(Map<String, String> executionStats) {
    _executionStats = executionStats;
  }

  public Map<String, String> getExecutionStats() {
    return _executionStats;
  }

  public int getStageId() {
    return _stageId;
  }

  public long getRequestId() {
    return _requestId;
  }

  public String getOperatorType() {
    return _operatorType;
  }

  // TODO: Return the string as a JSON string.
  @Override
  public String toString() {
    return String.format(
        "OperatorStats[requestId: %s, stageId %s, type: %s] ExecutionWallTime: %sms, No. Rows: %s, No. Block: %s",
        _requestId, _stageId, _operatorType, _executeStopwatch.elapsed(TimeUnit.MILLISECONDS), _numRows, _numBlock);
  }
}
