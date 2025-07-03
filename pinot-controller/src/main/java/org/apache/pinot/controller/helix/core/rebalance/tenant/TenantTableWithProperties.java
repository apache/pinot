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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to hold table properties when listing tables for a tenant during rebalancing.
 * This class contains pre-defined properties of a table that are relevant
 * for making include/exclude decisions during tenant rebalance operations.
 * The properties focus on factors that could impact rebalance performance and stability.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TenantTableWithProperties {
  // Basic table identification
  private static final Logger LOGGER = LoggerFactory.getLogger(TenantTableWithProperties.class);

  private String _tableNameWithType;
  private TableType _tableType;
  private boolean _isDimTable;
  private int _replication;
  private int _totalSegments;
  private long _estimatedTableSizeInBytes;
  private boolean _isUpsertEnabled;
  private boolean _isDedupEnabled;

  private static final int TABLE_SIZE_READER_TIMEOUT_MS = 10000; // 10 seconds

  @JsonCreator
  public TenantTableWithProperties(
      @JsonProperty("tableNameWithType") String tableNameWithType,
      @JsonProperty("tableType") TableType tableType,
      @JsonProperty("isDimTable") boolean isDimTable,
      @JsonProperty("replication") int replication,
      @JsonProperty("totalSegments") int totalSegments,
      @JsonProperty("estimatedTableSizeInBytes") long estimatedTableSizeInBytes,
      @JsonProperty("isUpsertEnabled") boolean isUpsertEnabled,
      @JsonProperty("isDedupEnabled") boolean isDedupEnabled) {
    _tableNameWithType = tableNameWithType;
    _tableType = tableType;
    _isDimTable = isDimTable;
    _replication = replication;
    _totalSegments = totalSegments;
    _estimatedTableSizeInBytes = estimatedTableSizeInBytes;
    _isUpsertEnabled = isUpsertEnabled;
    _isDedupEnabled = isDedupEnabled;
  }

  public TenantTableWithProperties(TableConfig tableConfig, Map<String, Map<String, String>> idealStateInstanceStateMap,
      TableSizeReader tableSizeReader) {
    _tableNameWithType = tableConfig.getTableName();
    _tableType = tableConfig.getTableType();
    _isDimTable = tableConfig.isDimTable();
    _replication = Integer.MAX_VALUE;
    for (Map<String, String> instanceState : idealStateInstanceStateMap.values()) {
      _replication = Math.min(instanceState.size(), _replication);
    }
    if (_replication == Integer.MAX_VALUE) {
      _replication = 0; // No instances available
    }
    try {
      _totalSegments = idealStateInstanceStateMap.size();
      TableSizeReader.TableSubTypeSizeDetails sizeDetails =
          tableSizeReader.getTableSubtypeSize(_tableNameWithType, TABLE_SIZE_READER_TIMEOUT_MS, false);
      _estimatedTableSizeInBytes = sizeDetails._estimatedSizeInBytes;
    } catch (InvalidConfigException e) {
      LOGGER.warn("Failed to read table size for table: {}", _tableNameWithType, e);
      _estimatedTableSizeInBytes = -1; // Indicate failure to read size
    }
    _isUpsertEnabled = tableConfig.isUpsertEnabled();
    _isDedupEnabled = tableConfig.isDedupEnabled();
  }

  // Basic table identification getters/setters
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public void setTableNameWithType(String tableNameWithType) {
    _tableNameWithType = tableNameWithType;
  }

  public TableType getTableType() {
    return _tableType;
  }

  public void setTableType(TableType tableType) {
    _tableType = tableType;
  }

  public boolean isDimTable() {
    return _isDimTable;
  }

  public void setDimTable(boolean dimTable) {
    _isDimTable = dimTable;
  }

  // Rebalance impact indicators getters/setters
  public int getReplication() {
    return _replication;
  }

  public void setReplication(int replication) {
    _replication = replication;
  }

  public int getTotalSegments() {
    return _totalSegments;
  }

  public void setTotalSegments(int totalSegments) {
    _totalSegments = totalSegments;
  }

  public long getEstimatedTableSizeInBytes() {
    return _estimatedTableSizeInBytes;
  }

  public void setEstimatedTableSizeInBytes(long estimatedTableSizeInBytes) {
    _estimatedTableSizeInBytes = estimatedTableSizeInBytes;
  }

  public boolean isUpsertEnabled() {
    return _isUpsertEnabled;
  }

  public void setUpsertEnabled(boolean upsertEnabled) {
    _isUpsertEnabled = upsertEnabled;
  }

  public boolean isDedupEnabled() {
    return _isDedupEnabled;
  }

  public void setDedupEnabled(boolean dedupEnabled) {
    _isDedupEnabled = dedupEnabled;
  }
}
