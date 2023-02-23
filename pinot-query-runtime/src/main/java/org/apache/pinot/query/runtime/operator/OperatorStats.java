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
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.operator.utils.OperatorUtils;


public class OperatorStats {
  private final Stopwatch _executeStopwatch = Stopwatch.createUnstarted();

  // TODO: add a operatorId for better tracking purpose.
  private final int _stageId;
  private final long _requestId;
  private final VirtualServerAddress _serverAddress;

  private final String _operatorType;

  private int _numBlock = 0;
  private int _numRows = 0;
  private Map<String, String> _executionStats;

  public OperatorStats(long requestId, int stageId, VirtualServerAddress serverAddress, String operatorType) {
    _stageId = stageId;
    _requestId = requestId;
    _serverAddress = serverAddress;
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

  public void recordSingleStat(String key, String stat) {
    _executionStats.put(key, stat);
  }

  public void recordExecutionStats(Map<String, String> executionStats) {
    _executionStats.putAll(executionStats);
  }

  public Map<String, String> getExecutionStats() {
    _executionStats.putIfAbsent(OperatorUtils.NUM_BLOCKS, String.valueOf(_numBlock));
    _executionStats.putIfAbsent(OperatorUtils.NUM_ROWS, String.valueOf(_numRows));
    _executionStats.putIfAbsent(OperatorUtils.THREAD_EXECUTION_TIME,
        String.valueOf(_executeStopwatch.elapsed(TimeUnit.MILLISECONDS)));
    return _executionStats;
  }

  public int getStageId() {
    return _stageId;
  }

  public long getRequestId() {
    return _requestId;
  }

  public VirtualServerAddress getServerAddress() {
    return _serverAddress;
  }

  public String getOperatorType() {
    return _operatorType;
  }

  @Override
  public String toString() {
    return OperatorUtils.operatorStatsToJson(this);
  }
}
