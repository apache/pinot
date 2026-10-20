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
package org.apache.pinot.coordination.ratis.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;


/// Administrator-owned server eligibility for the bounded ordinary OFFLINE placement protocol. Participant
/// endpoint advertisements cannot grant tenant or failure-domain authority. Policy changes apply without restart.
public record NativeReplicaPlacementPolicy(Map<String, Server> servers) {
  public static final String CONFIG_KEY = "pinot.coordination.ratis.offline.placement";
  public static final int MAX_SERVERS = 64;
  public static final int MAX_REPLICAS = 16;

  public NativeReplicaPlacementPolicy {
    servers = Map.copyOf(servers);
  }

  public record Server(String tenant, String failureDomain) {
  }

  public static NativeReplicaPlacementPolicy parse(String json) {
    require(json != null && !json.isBlank(), "Native OFFLINE placement policy is not configured");
    try {
      JsonNode root = MetadataProtocol.decodeValue(json.getBytes(StandardCharsets.UTF_8), JsonNode.class);
      require(root.isObject() && root.size() == 1 && root.path("servers").isObject(),
          "Placement policy requires only a servers object");
      JsonNode values = root.path("servers");
      require(values.size() <= MAX_SERVERS, "Placement policy exceeds its server bound");
      Map<String, Server> servers = new TreeMap<>();
      for (var entry : values.properties()) {
        OfflineSegmentCatalog.identifier(entry.getKey(), MembershipProtocol.MAX_IDENTIFIER_LENGTH);
        JsonNode value = entry.getValue();
        require(value.isObject() && value.size() == 2 && value.path("tenant").isTextual()
            && value.path("failureDomain").isTextual(), "Server policy requires tenant and failureDomain strings");
        String tenant = value.path("tenant").textValue();
        String domain = value.path("failureDomain").textValue();
        OfflineSegmentCatalog.identifier(tenant, 1024);
        OfflineSegmentCatalog.identifier(domain, 1024);
        servers.put(entry.getKey(), new Server(tenant, domain));
      }
      return new NativeReplicaPlacementPolicy(servers);
    } catch (IOException e) {
      throw new CoordinationRejection(Status.INVALID_REQUEST, "Invalid native OFFLINE placement policy JSON");
    }
  }

  @SuppressWarnings("deprecation") // Legacy wire constraints must be rejected instead of silently ignored.
  public static int validateTable(TableConfig table) {
    require(table != null && table.getTableType() == TableType.OFFLINE && !table.isDimTable()
        && !table.isUpsertEnabled() && table.getDedupConfig() == null
        && table.getTenantConfig() != null && table.getTenantConfig().getTagOverrideConfig() == null
        && (table.getTierConfigsList() == null || table.getTierConfigsList().isEmpty())
        && (table.getInstanceAssignmentConfigMap() == null || table.getInstanceAssignmentConfigMap().isEmpty())
        && (table.getInstancePartitionsMap() == null || table.getInstancePartitionsMap().isEmpty())
        && (table.getSegmentAssignmentConfigMap() == null || table.getSegmentAssignmentConfigMap().isEmpty())
        && table.getValidationConfig() != null && table.getValidationConfig().getPeerSegmentDownloadScheme() == null
        && table.getValidationConfig().getReplicaGroupStrategyConfig() == null
        && table.getValidationConfig().getReplicasPerPartition() == null
        && ordinaryRouting(table.getRoutingConfig()),
        "Native placement supports ordinary OFFLINE tables without tag overrides, tiers or Helix assignment options");
    tenant(table);
    try {
      int replication = table.getReplication();
      require(replication > 0 && replication <= MAX_REPLICAS, "Native placement replication must be between 1 and 16");
      return replication;
    } catch (NumberFormatException e) {
      throw new CoordinationRejection(Status.INVALID_REQUEST, "Native placement requires explicit integer replication");
    }
  }

  public static String tenant(TableConfig table) {
    require(table != null && table.getTenantConfig() != null, "Native placement requires a server tenant");
    // Pinot's existing tenant convention maps an omitted server tenant to DefaultTenant.
    String tenant = table.getTenantConfig().getServer() == null ? "DefaultTenant" : table.getTenantConfig().getServer();
    OfflineSegmentCatalog.identifier(tenant, 1024);
    return tenant;
  }

  private static boolean ordinaryRouting(RoutingConfig routing) {
    return routing == null || routing.getRoutingTableBuilderName() == null
        && (routing.getInstanceSelectorType() == null
            || RoutingConfig.BALANCED_INSTANCE_SELECTOR_TYPE.equals(routing.getInstanceSelectorType()))
        && !Boolean.TRUE.equals(routing.getUseFixedReplica());
  }

  public static UUID operationId(UUID historyId, UUID materializationId) {
    require(historyId != null && materializationId != null, "Placement operation requires complete identities");
    return UUID.nameUUIDFromBytes((historyId + "\0NATIVE_REPLICA_RECONCILE\0" + materializationId)
        .getBytes(StandardCharsets.UTF_8));
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new CoordinationRejection(Status.INVALID_REQUEST, message);
    }
  }
}
