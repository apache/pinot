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
import java.util.HashSet;
import java.util.List;
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


/// Checks whether an upsert table is within its configured per-partition primary key quota
/// ([QuotaConfig#getMaxPrimaryKeysPerPartition()]).
///
/// The per-partition primary key map is the real heap unit for upsert tables: each partition's
/// `primaryKey -> RecordLocation` map lives wholly on one server per replica. Therefore the cap is applied per
/// partition, and a breach is when the count of any single partition (taken as the max across the replicas that host
/// it) exceeds the cap. Taking the max — rather than summing across replicas — is correct because the cap bounds a
/// single replica's map, and it also tolerates transient double-hosting during rebalance and replica lag.
///
/// The counts are read from servers via [ServerSegmentMetadataReader#getAggregatedTableMetadataFromServer], which
/// returns `partitionId -> {serverId -> primaryKeyCount}` with one count per replica. Servers that fail to respond are
/// silently left out of that map, so a missing server can only hide a breach, never cause one.
///
/// The check therefore fails closed: unless every server hosting the table (non-OFFLINE in the IdealState) reports its
/// counts, a check that finds no breach returns [Status#UNDETERMINED] rather than [Status#WITHIN_QUOTA]. A table
/// already paused by this quota stays paused, even after its cap is raised, while any of those servers is down or has
/// not loaded its upsert partitions yet. Resuming consumption through the admin API
/// (`/tables/{tableName}/resumeConsumption`) overrides this: the table keeps consuming while the status is
/// undetermined, and is paused again only if a later check sees a breach.
///
/// Note: a pause triggered by this quota is sticky. Primary key counts do not drop while consumption is paused, and
/// `metadataTTL` does not help either: the TTL is measured against the largest comparison value consumed, which stops
/// moving when consumption stops, and expired keys are only swept when a new consuming segment starts. The count only
/// falls when segments are removed (e.g. by retention) or a server restarts and reloads its segments, so in practice
/// raising the cap is what resumes the table.
public class PrimaryKeyQuotaChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(PrimaryKeyQuotaChecker.class);

  /// Outcome of a primary key quota check. [#UNDETERMINED] is returned when the counts could not be reliably read
  /// (e.g. servers timed out or returned nothing); callers should treat it as "keep the current state" rather than
  /// resume a paused table, since resuming on a server outage would defeat the quota exactly when it matters.
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

  /// Resolves the effective primary key quota enforcement mode for a table: DISABLED if the controller-wide primary key
  /// quota check is turned off, the table is not an upsert table, or no cap is configured; otherwise the table's own
  /// mode, defaulting to ENFORCED when the table does not set one.
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

  /// Checks whether the table is within its per-partition primary key quota. Emits the
  /// [ControllerMeter#PRIMARY_KEY_QUOTA_EXCEEDED] meter on a breach regardless of enforcement mode (SHADOW or
  /// ENFORCED); callers use [#getEnforcementMode(TableConfig)] to decide whether to act on the breach.
  public Status checkPrimaryKeyQuota(TableConfig tableConfig) {
    if (getEnforcementMode(tableConfig) == QuotaEnforcementMode.DISABLED) {
      // No cap / not applicable / disabled: skip entirely (no server calls)
      return Status.WITHIN_QUOTA;
    }
    String tableNameWithType = tableConfig.getTableName();
    long cap = tableConfig.getQuotaConfig().getMaxPrimaryKeysPerPartition();

    Set<String> servers;
    Map<Integer, Map<String, Long>> partitionToServerPkCount;
    try {
      servers = _pinotHelixResourceManager.getServerToSegmentsMap(tableNameWithType).keySet();
      if (servers.isEmpty()) {
        return Status.UNDETERMINED;
      }
      BiMap<String, String> endpoints = _pinotHelixResourceManager.getDataInstanceAdminEndpoints(servers);
      int numReplicas = _pinotHelixResourceManager.getNumReplicas(tableConfig);
      TableMetadataInfo tableMetadataInfo = _serverSegmentMetadataReader.getAggregatedTableMetadataFromServer(
          tableNameWithType, endpoints, List.of(), numReplicas, _timeoutMs);
      partitionToServerPkCount = tableMetadataInfo.getPartitionToServerPrimaryKeyCountMap();
    } catch (Exception e) {
      LOGGER.warn("Failed to read primary key counts for table: {}, skipping primary key quota check",
          tableNameWithType, e);
      return Status.UNDETERMINED;
    }

    if (partitionToServerPkCount == null || partitionToServerPkCount.isEmpty()) {
      return Status.UNDETERMINED;
    }

    long maxPartitionPkCount = 0;
    int worstPartition = -1;
    String worstServer = null;
    Set<String> respondedServers = new HashSet<>();
    for (Map.Entry<Integer, Map<String, Long>> partitionEntry : partitionToServerPkCount.entrySet()) {
      for (Map.Entry<String, Long> serverEntry : partitionEntry.getValue().entrySet()) {
        respondedServers.add(serverEntry.getKey());
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
    // A breach seen in a partial response is real, but the absence of one is not: the missing servers may host the
    // partition that is over the cap
    Set<String> missingServers = new HashSet<>(servers);
    missingServers.removeAll(respondedServers);
    if (!missingServers.isEmpty()) {
      LOGGER.warn("Primary key counts for table: {} are missing from servers: {}, cannot confirm it is within the "
          + "primary key quota", tableNameWithType, missingServers);
      return Status.UNDETERMINED;
    }
    return Status.WITHIN_QUOTA;
  }

  /// Convenience boolean wrapper. Returns true only when the quota is definitively exceeded; UNDETERMINED maps to false
  /// so that callers do not treat a failed read as a breach.
  public boolean isTablePrimaryKeyQuotaExceeded(TableConfig tableConfig) {
    return checkPrimaryKeyQuota(tableConfig) == Status.EXCEEDED;
  }

  private void recordBreach(String tableNameWithType) {
    _controllerMetrics.addMeteredTableValue(tableNameWithType, ControllerMeter.PRIMARY_KEY_QUOTA_EXCEEDED, 1L);
  }
}
