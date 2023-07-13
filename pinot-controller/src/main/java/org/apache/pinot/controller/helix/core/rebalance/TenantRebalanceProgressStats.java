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
package org.apache.pinot.controller.helix.core.rebalance;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;


public class TenantRebalanceProgressStats {
  // status map of tables and their respective rebalance status
  private Map<String, String> _tableStatusMap;
  private final Map<String, String> _tableRebalanceJobIdMap = new HashMap<>();
  private int _totalTables;
  private int _remainingTables;
  // When did Rebalance start
  private long _startTimeMs;
  // How long did rebalance take
  private long _timeToFinishInSeconds;
  // Success/failure message
  private String _completionStatusMsg;

  public TenantRebalanceProgressStats() {
  }

  public TenantRebalanceProgressStats(Set<String> tables) {
    _tableStatusMap = tables.stream()
        .collect(Collectors.toMap(Function.identity(), k -> TableStatus.UNPROCESSED.name()));
    _totalTables = tables.size();
  }

  public Map<String, String> getTableStatusMap() {
    return _tableStatusMap;
  }

  public void setTableStatusMap(Map<String, String> tableStatusMap) {
    _tableStatusMap = tableStatusMap;
  }

  public int getTotalTables() {
    return _totalTables;
  }

  public void setTotalTables(int totalTables) {
    _totalTables = totalTables;
  }

  public int getRemainingTables() {
    return _remainingTables;
  }

  public void setRemainingTables(int remainingTables) {
    _remainingTables = remainingTables;
  }

  public long getStartTimeMs() {
    return _startTimeMs;
  }

  public void setStartTimeMs(long startTimeMs) {
    _startTimeMs = startTimeMs;
  }

  public long getTimeToFinishInSeconds() {
    return _timeToFinishInSeconds;
  }

  public void setTimeToFinishInSeconds(long timeToFinishInSeconds) {
    _timeToFinishInSeconds = timeToFinishInSeconds;
  }

  public String getCompletionStatusMsg() {
    return _completionStatusMsg;
  }

  public void setCompletionStatusMsg(String completionStatusMsg) {
    _completionStatusMsg = completionStatusMsg;
  }

  public void updateTableStatus(String tableName, String status) {
    _tableStatusMap.put(tableName, status);
  }

  public void putTableRebalanceJobId(String tableName, String jobId) {
    _tableRebalanceJobIdMap.put(tableName, jobId);
  }

  public Map<String, String> getTableRebalanceJobIdMap() {
    return _tableRebalanceJobIdMap;
  }

  public enum TableStatus {
    UNPROCESSED, PROCESSING, PROCESSED
  }
}
