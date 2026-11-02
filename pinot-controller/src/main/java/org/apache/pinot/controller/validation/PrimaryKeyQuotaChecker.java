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
package org.apache.pinot.controller.validation;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.TableMetadataInfo;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.ServerSegmentMetadataReader;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.QuotaEnforcementMode;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Checks whether an upsert table is within its configured per-partition primary key quota
 * ({@link QuotaConfig#getMaxPrimaryKeysPerPartition()}).
 *
 * <p>The per-partition primary key map is the real heap unit for upsert tables: each partition's
 * {@code primaryKey -> RecordLocation} map lives wholly on one server per replica. Therefore the cap is applied
 * per partition, and a breach is when the count of any single partition (taken as the max across the replicas that
 * host it) exceeds the cap. Taking the max — rather than summing across replicas — is correct because the cap bounds
 * a single replica's map, and it also tolerates transient double-hosting during rebalance and replica lag.
 *
 * <p>The counts are read from servers via {@link ServerSegmentMetadataReader#getAggregatedTableMetadataFromServer},
 * which returns {@code partitionId -> {serverId -> primaryKeyCount}} with one count per replica.
 *
 * <p>Note: primary key counts do not drop when consumption is paused (keys are only evicted by TTL/compaction or when
 * segments are removed), so a pause triggered by this quota is sticky until the table falls back under the cap or the
 * cap is raised.
 */
public class PrimaryKeyQuotaChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(PrimaryKeyQuotaChecker.class);

  /**
   * Outcome of a primary key quota check. {@link #UNDETERMINED} is returned when the counts could not be reliably
   * read (e.g. servers timed out or returned nothing); callers should treat it as "keep the current state" rather
   * than resume a paused table, since resuming on a server outage would defeat the quota exactly when it matters.
   */
  public enum Status {
    WITHIN_QUOTA, EXCEEDED, UNDETERMINED
  }

  private final ServerSegmentMetadataReader _serverSegmentMetadataReader;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final ControllerMetrics _controllerMetrics;
  private final boolean _isEnabled;
  private final int _timeoutMs;

  public PrimaryKeyQuotaChecker(ServerSegmentMetadataReader serverSegmentMetadataReader,
      PinotHelixResourceManager pinotHelixResourceManager, ControllerMetrics controllerMetrics,
      ControllerConf controllerConf) {
    _serverSegmentMetadataReader = serverSegmentMetadataReader;
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _controllerMetrics = controllerMetrics;
    _isEnabled = controllerConf.getEnablePrimaryKeyQuotaCheck();
    _timeoutMs = controllerConf.getServerAdminRequestTimeoutSeconds() * 1000;
    Preconditions.checkArgument(_timeoutMs > 0, "Timeout value must be > 0, input: %s", _timeoutMs);
  }

  /**
   * Resolves the effective primary key quota enforcement mode for a table: DISABLED if the controller-wide primary key
   * quota check is turned off, the table is not an upsert table, or no cap is configured; otherwise the table's own
   * mode, defaulting to ENFORCED when the table does not set one.
   */
  public QuotaEnforcementMode getEnforcementMode(TableConfig tableConfig) {
    if (!_isEnabled || !tableConfig.isUpsertEnabled()) {
      return QuotaEnforcementMode.DISABLED;
    }
    QuotaConfig quotaConfig = tableConfig.getQuotaConfig();
    if (quotaConfig == null || quotaConfig.getMaxPrimaryKeysPerPartition() == null) {
      return QuotaEnforcementMode.DISABLED;
    }
    return quotaConfig.getPrimaryKeyQuotaEnforcementMode() != null ? quotaConfig.getPrimaryKeyQuotaEnforcementMode()
        : QuotaEnforcementMode.ENFORCED;
  }

  /**
   * Checks whether the table is within its per-partition primary key quota. Emits the
   * {@link ControllerMeter#PRIMARY_KEY_QUOTA_EXCEEDED} meter on a breach regardless of enforcement mode (SHADOW or
   * ENFORCED); callers use {@link #getEnforcementMode(TableConfig)} to decide whether to act on the breach.
   */
  public Status checkPrimaryKeyQuota(TableConfig tableConfig) {
    if (getEnforcementMode(tableConfig) == QuotaEnforcementMode.DISABLED) {
      // No cap / not applicable / disabled: skip entirely (no server calls)
      return Status.WITHIN_QUOTA;
    }
    String tableNameWithType = tableConfig.getTableName();
    long cap = tableConfig.getQuotaConfig().getMaxPrimaryKeysPerPartition();

    Map<Integer, Map<String, Long>> partitionToServerPkCount;
    try {
      Set<String> servers = _pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType).keySet();
      if (servers.isEmpty()) {
        return Status.UNDETERMINED;
      }
      BiMap<String, String> endpoints = _pinotHelixResourceManager.getDataInstanceAdminEndpoints(servers);
      int numReplicas = _pinotHelixResourceManager.getNumReplicas(tableConfig);
      TableMetadataInfo tableMetadataInfo = _serverSegmentMetadataReader.getAggregatedTableMetadataFromServer(
          tableNameWithType, endpoints, Collections.emptyList(), numReplicas, _timeoutMs);
      partitionToServerPkCount = tableMetadataInfo.getPartitionToServerPrimaryKeyCountMap();
    } catch (Exception e) {
      LOGGER.warn("Failed to read primary key counts for table: {}, skipping primary key quota check", tableNameWithType,
          e);
      return Status.UNDETERMINED;
    }

    if (partitionToServerPkCount == null || partitionToServerPkCount.isEmpty()) {
      return Status.UNDETERMINED;
    }

    long maxPartitionPkCount = 0;
    int worstPartition = -1;
    String worstServer = null;
    for (Map.Entry<Integer, Map<String, Long>> partitionEntry : partitionToServerPkCount.entrySet()) {
      for (Map.Entry<String, Long> serverEntry : partitionEntry.getValue().entrySet()) {
        long count = serverEntry.getValue();
        if (count > maxPartitionPkCount) {
          maxPartitionPkCount = count;
          worstPartition = partitionEntry.getKey();
          worstServer = serverEntry.getKey();
        }
      }
    }

    if (maxPartitionPkCount > cap) {
      LOGGER.warn("Primary key quota exceeded for table: {}. Partition: {} on server: {} has {} primary keys, "
          + "configured cap per partition: {}", tableNameWithType, worstPartition, worstServer, maxPartitionPkCount,
          cap);
      recordBreach(tableNameWithType);
      return Status.EXCEEDED;
    }
    return Status.WITHIN_QUOTA;
  }

  /**
   * Convenience boolean wrapper. Returns true only when the quota is definitively exceeded; UNDETERMINED maps to false
   * so that callers do not treat a failed read as a breach.
   */
  public boolean isTablePrimaryKeyQuotaExceeded(TableConfig tableConfig) {
    return checkPrimaryKeyQuota(tableConfig) == Status.EXCEEDED;
  }

  private void recordBreach(String tableNameWithType) {
    _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.PRIMARY_KEY_QUOTA_EXCEEDED, 1L);
  }
}
