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
import java.util.concurrent.TimeUnit;


public class OperatorStats {
  private final Stopwatch _executeStopwatch = Stopwatch.createUnstarted();

  // TODO: add a operatorId for better tracking purpose.
  private final int _stageId;
  private final long _requestId;

  private final String _operatorType;

  private int _numInputBlock = 0;
  private int _numInputRows = 0;

  private int _numOutputBlock = 0;

  private int _numOutputRows = 0;

  public OperatorStats(long requestId, int stageId, String operatorType) {
    _stageId = stageId;
    _requestId = requestId;
    _operatorType = operatorType;
  }

  public void startTimer() {
    _executeStopwatch.start();
  }

  public void endTimer() {
    _executeStopwatch.stop();
  }

  public void recordInput(int numBlock, int numRows) {
    _numInputBlock += numBlock;
    _numInputRows += numRows;
  }

  public void recordOutput(int numBlock, int numRows) {
    _numOutputBlock += numBlock;
    _numOutputRows += numRows;
  }

  @Override
  public String toString() {
    return String.format(
        "OperatorStats[type: %s, requestId: %s, stageId %s] ExecutionWallTime: %sms, InputRows: %s, InputBlock: "
            + "%s, OutputRows: %s, OutputBlock: %s", _operatorType, _requestId, _stageId,
        _executeStopwatch.elapsed(TimeUnit.MILLISECONDS), _numInputRows, _numInputBlock, _numOutputRows,
        _numOutputBlock);
  }
}
