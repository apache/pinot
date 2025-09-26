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
package org.apache.pinot.controller.helix.core.rebalance.tenant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceJobConstants;
import org.apache.pinot.spi.utils.JsonUtils;


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
    Preconditions.checkState(tables != null && !tables.isEmpty(), "List of tables to observe is empty.");
    _tableStatusMap = tables.stream()
        .collect(Collectors.toMap(Function.identity(), k -> TableStatus.IN_QUEUE.name()));
    _totalTables = tables.size();
    _remainingTables = _totalTables;
  }

  public TenantRebalanceProgressStats(TenantRebalanceProgressStats other) {
    _tableStatusMap = new HashMap<>(other._tableStatusMap);
    _tableRebalanceJobIdMap.putAll(other._tableRebalanceJobIdMap);
    _totalTables = other._totalTables;
    _remainingTables = other._remainingTables;
    _startTimeMs = other._startTimeMs;
    _timeToFinishInSeconds = other._timeToFinishInSeconds;
    _completionStatusMsg = other._completionStatusMsg;
  }

  @Nullable
  public static TenantRebalanceProgressStats fromTenantRebalanceJobMetadata(Map<String, String> jobMetadata)
      throws JsonProcessingException {
    String tenantRebalanceContextStr = jobMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_PROGRESS_STATS);
    if (StringUtils.isEmpty(tenantRebalanceContextStr)) {
      return null;
    }
    return JsonUtils.stringToObject(tenantRebalanceContextStr, TenantRebalanceProgressStats.class);
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
    IN_QUEUE,
    REBALANCING,
    DONE,
    CANCELLED, // cancelled by user
    ABORTED, // cancelled by TenantRebalanceChecker
    NOT_SCHEDULED // tables IN_QUEUE will be marked as NOT_SCHEDULED once the rebalance job is cancelled/aborted
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TenantRebalanceProgressStats)) {
      return false;
    }
    TenantRebalanceProgressStats that = (TenantRebalanceProgressStats) o;
    return _totalTables == that._totalTables && _remainingTables == that._remainingTables
        && _startTimeMs == that._startTimeMs && _timeToFinishInSeconds == that._timeToFinishInSeconds
        && Objects.equals(_tableStatusMap, that._tableStatusMap) && Objects.equals(
        _tableRebalanceJobIdMap, that._tableRebalanceJobIdMap) && Objects.equals(_completionStatusMsg,
        that._completionStatusMsg);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_tableStatusMap, _tableRebalanceJobIdMap, _totalTables, _remainingTables, _startTimeMs,
        _timeToFinishInSeconds, _completionStatusMsg);
  }
}
