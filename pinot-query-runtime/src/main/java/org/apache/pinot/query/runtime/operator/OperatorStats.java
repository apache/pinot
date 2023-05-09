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
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.utils.OperatorUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


public class OperatorStats {
  private final Stopwatch _executeStopwatch = Stopwatch.createUnstarted();

  // TODO: add a operatorId for better tracking purpose.
  private final int _stageId;
  private final long _requestId;

  private final VirtualServerAddress _serverAddress;

  private int _numBlock = 0;
  private int _numRows = 0;
  private long _startTimeMs = -1;
  private long _endTimeMs = -1;
  private final Map<String, String> _executionStats;
  private boolean _processingStarted = false;

  public OperatorStats(OpChainExecutionContext context) {
    this(context.getRequestId(), context.getStageId(), context.getServer());
  }

  //TODO: remove this constructor after the context constructor can be used in serialization and deserialization
  public OperatorStats(long requestId, int stageId, VirtualServerAddress serverAddress) {
    _stageId = stageId;
    _requestId = requestId;
    _serverAddress = serverAddress;
    _executionStats = new HashMap<>();
  }

  public void startTimer() {
    _startTimeMs = _startTimeMs == -1 ? System.currentTimeMillis() : _startTimeMs;
    if (!_executeStopwatch.isRunning()) {
      _executeStopwatch.start();
    }
  }

  public void endTimer(TransferableBlock block) {
    if (_executeStopwatch.isRunning()) {
      _executeStopwatch.stop();
      _endTimeMs = System.currentTimeMillis();
    }
    if (!_processingStarted && block.isNoOpBlock()) {
      _startTimeMs = -1;
      _endTimeMs = -1;
      _executeStopwatch.reset();
    } else {
      _processingStarted = true;
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
    _executionStats.putIfAbsent(DataTable.MetadataKey.NUM_BLOCKS.getName(), String.valueOf(_numBlock));
    _executionStats.putIfAbsent(DataTable.MetadataKey.NUM_ROWS.getName(), String.valueOf(_numRows));
    _executionStats.putIfAbsent(DataTable.MetadataKey.OPERATOR_EXECUTION_TIME_MS.getName(),
        String.valueOf(_executeStopwatch.elapsed(TimeUnit.MILLISECONDS)));
    // wall time are recorded slightly longer than actual execution but it is OK.

    if (_startTimeMs != -1) {
      _executionStats.putIfAbsent(DataTable.MetadataKey.OPERATOR_EXEC_START_TIME_MS.getName(),
          String.valueOf(_startTimeMs));
      long endTimeMs = _endTimeMs == -1 ? System.currentTimeMillis() : _endTimeMs;
      _executionStats.putIfAbsent(DataTable.MetadataKey.OPERATOR_EXEC_END_TIME_MS.getName(),
          String.valueOf(endTimeMs));
    }
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

  @Override
  public String toString() {
    return OperatorUtils.operatorStatsToJson(this);
  }
}
