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
package org.apache.pinot.controller.api.resources;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.CommonConstants;


public class TableDeletionRequest {
  private String _rawTableName;
  private String _tableType;
  private Set<String> _instancesForRealtimeTable;
  private List<String> _tablesDeleted;
  private long _offlineTableDeletionStartTime;
  private long _offlineTableIdealStateRemovalEndTime;
  private long _realtimeTableDeletionStartTime;
  private long _realtimeTableIdealStateRemovalEndTime;

  public TableDeletionRequest(String tableName, String tableTypeStr) {
    if (tableTypeStr == null) {
      CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
      if (tableType != null) {
        tableTypeStr = tableType.name();
      }
    }
    _rawTableName = TableNameBuilder.extractRawTableName(tableName);
    _tableType = tableTypeStr;
    _tablesDeleted = new LinkedList<>();
    _offlineTableDeletionStartTime = 0L;
    _realtimeTableDeletionStartTime = 0L;
  }

  public String getRawTableName() {
    return _rawTableName;
  }

  public String getTableType() {
    return _tableType;
  }

  public void setInstancesForRealtimeTable(Set<String> instancesForRealtimeTable) {
    this._instancesForRealtimeTable = instancesForRealtimeTable;
  }

  public Set<String> getInstancesForRealtimeTable() {
    return _instancesForRealtimeTable;
  }

  public List<String> getTablesDeleted() {
    return _tablesDeleted;
  }

  public long getOfflineTableDeletionStartTime() {
    return _offlineTableDeletionStartTime;
  }

  public void setOfflineTableDeletionStartTime(long offlineTableDeletionStartTime) {
    _offlineTableDeletionStartTime = offlineTableDeletionStartTime;
  }

  public long getOfflineTableIdealStateRemovalEndTime() {
    return _offlineTableIdealStateRemovalEndTime;
  }

  public void setOfflineTableIdealStateRemovalEndTime(long offlineTableIdealStateRemovalEndTime) {
    _offlineTableIdealStateRemovalEndTime = offlineTableIdealStateRemovalEndTime;
  }

  public long getRealtimeTableDeletionStartTime() {
    return _realtimeTableDeletionStartTime;
  }

  public void setRealtimeTableDeletionStartTime(long realtimeTableDeletionStartTime) {
    _realtimeTableDeletionStartTime = realtimeTableDeletionStartTime;
  }

  public long getRealtimeTableIdealStateRemovalEndTime() {
    return _realtimeTableIdealStateRemovalEndTime;
  }

  public void setRealtimeTableIdealStateRemovalEndTime(long realtimeTableIdealStateRemovalEndTime) {
    _realtimeTableIdealStateRemovalEndTime = realtimeTableIdealStateRemovalEndTime;
  }
}
