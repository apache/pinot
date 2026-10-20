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
package org.apache.pinot.controller.util;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Preserves the existing Helix/ZK table-size metadata reads and live lead-controller metrics check. A captured
/// input is immutable, but Helix's separate config/assignment reads do not provide a cross-record atomic snapshot.
public final class HelixTableSizeMetadataProvider implements TableSizeMetadataProvider {
  private final PinotHelixResourceManager _resourceManager;
  private final LeadControllerManager _leadControllerManager;

  public HelixTableSizeMetadataProvider(PinotHelixResourceManager resourceManager,
      LeadControllerManager leadControllerManager) {
    _resourceManager = resourceManager;
    _leadControllerManager = leadControllerManager;
  }

  @Override
  public Snapshot readTableSizes(String tableName, boolean includeReplacedSegments) throws InvalidConfigException {
    TableConfig offline = ZKMetadataProvider.getOfflineTableConfig(_resourceManager.getPropertyStore(), tableName);
    TableConfig realtime = ZKMetadataProvider.getRealtimeTableConfig(_resourceManager.getPropertyStore(), tableName);
    Table realtimeTable = table(TableNameBuilder.REALTIME.tableNameWithType(tableName), realtime,
        includeReplacedSegments);
    Table offlineTable = table(TableNameBuilder.OFFLINE.tableNameWithType(tableName), offline,
        includeReplacedSegments);
    return new Snapshot(offlineTable, realtimeTable);
  }

  @Nullable
  private Table table(String tableNameWithType, @Nullable TableConfig config, boolean includeReplacedSegments)
      throws InvalidConfigException {
    return config == null ? null : new Table(tableNameWithType, _resourceManager.getNumReplicas(config),
        config.getIndexingConfig() != null && config.getIndexingConfig().isCompressionStatsEnabled(),
        readReplicaAssignments(tableNameWithType, includeReplacedSegments));
  }

  @Override
  public Replicas readReplicaAssignments(String tableNameWithType, boolean includeReplacedSegments)
      throws InvalidConfigException {
    Map<String, List<String>> assignments = _resourceManager.getServerToSegmentsMap(tableNameWithType, null,
        includeReplacedSegments);
    return new Replicas(assignments, _resourceManager.getDataInstanceAdminEndpoints(assignments.keySet()),
        _resourceManager.getServerAdminAuthProvider());
  }

  @Override
  public boolean canPublishMetrics(String tableNameWithType) {
    return _leadControllerManager.isLeaderForTable(tableNameWithType);
  }
}
