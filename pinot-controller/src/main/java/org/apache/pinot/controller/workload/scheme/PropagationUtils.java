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
package org.apache.pinot.controller.workload.scheme;

import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * PropagationUtils is used to get the mapping between
 * 1. Table to Helix tags
 * 2. Helix tags to instances
 * 3. Instance to Helix tags
 * 4. Helix tags to workload configs
 */
public class PropagationUtils {

  private PropagationUtils() {
  }

  /**
   * Get the mapping table → {NON_LEAF_NODE→brokerTag, LEAF_NODE→(serverTag + overrides)}
   * 1. Get all table configs from the PinotHelixResourceManager
   * 2. For each table config, extract the tenant config
   * 3. For each tenant config, get the broker and server tags
   * 4. Populate the helix tags for NON_LEAF_NODE and LEAF_NODE separately
   */
  public static Map<String, Map<NodeConfig.Type, Set<String>>> getTableToHelixTags(
          PinotHelixResourceManager pinotResourceManager) {
    Map<String, Map<NodeConfig.Type, Set<String>>> tableToTags = new HashMap<>();
    for (TableConfig tableConfig : pinotResourceManager.getAllTableConfigs()) {
      TenantConfig tenantConfig = tableConfig.getTenantConfig();
      TableType tableType = tableConfig.getTableType();

      // Gather all relevant tags for this tenant
      Set<String> tenantTags = new HashSet<>();
      collectTenantTags(tenantTags, tenantConfig, tableType);

      // Populate the helix tags for NON_LEAF_NODE and LEAF_NODE separately to provide flexibility
      // in workload propagation to either leaf nodes or non-leaf nodes
      String brokerTag = TagNameUtils.getBrokerTagForTenant(tenantConfig.getBroker());
      Set<String> nonLeafTags = Collections.singleton(brokerTag);

      Set<String> leafTags = new HashSet<>(tenantTags);
      leafTags.remove(brokerTag);

      Map<NodeConfig.Type, Set<String>> nodeTypeToTags = new EnumMap<>(NodeConfig.Type.class);
      nodeTypeToTags.put(NodeConfig.Type.NON_LEAF_NODE, nonLeafTags);
      nodeTypeToTags.put(NodeConfig.Type.LEAF_NODE, leafTags);

      tableToTags.put(tableConfig.getTableName(), nodeTypeToTags);
    }
    return tableToTags;
  }

  private static void collectTenantTags(Set<String> tagSet, TenantConfig tenantConfig, TableType tableType) {
    tagSet.add(TagNameUtils.getBrokerTagForTenant(tenantConfig.getBroker()));
    if (tableType == TableType.OFFLINE) {
      tagSet.add(TagNameUtils.getOfflineTagForTenant(tenantConfig.getServer()));
    } else {
      tagSet.add(TagNameUtils.getRealtimeTagForTenant(tenantConfig.getServer()));
    }
    TagOverrideConfig overrideConfig = tenantConfig.getTagOverrideConfig();
    if (overrideConfig != null) {
      if (overrideConfig.getRealtimeCompleted() != null) {
        tagSet.add(overrideConfig.getRealtimeCompleted());
      }
      if (overrideConfig.getRealtimeConsuming() != null) {
        tagSet.add(overrideConfig.getRealtimeConsuming());
      }
    }
  }

  /**
   * Get the helix tags for a given table name.
   * If the table name does not have a type suffix, it will return both offline and realtime tags.
   */
  public static Set<String> getHelixTagsForTable(PinotHelixResourceManager pinotResourceManager,
          String tableName) {
    Set<String> combinedTags = new HashSet<>();
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    List<String> tablesWithType = (tableType == null)
        ? Arrays.asList(TableNameBuilder.OFFLINE.tableNameWithType(tableName),
            TableNameBuilder.REALTIME.tableNameWithType(tableName))
        : Collections.singletonList(tableName);

    for (String table : tablesWithType) {
      TableConfig tableConfig = pinotResourceManager.getTableConfig(table);
      if (tableConfig != null) {
        collectTenantTags(combinedTags, tableConfig.getTenantConfig(), tableConfig.getTableType());
      }
    }
    return combinedTags;
  }

  /**
   * Get the mapping between helix tag -> instances
   */
  public static Map<String, Set<String>> getHelixTagToInstances(PinotHelixResourceManager pinotResourceManager) {
    Map<String, Set<String>> tagToInstances = new HashMap<>();
    for (InstanceConfig instanceConfig : pinotResourceManager.getAllHelixInstanceConfigs()) {
      String instanceName = instanceConfig.getInstanceName();
      for (String helixTag : instanceConfig.getTags()) {
        tagToInstances.computeIfAbsent(helixTag, tag -> new HashSet<>()).add(instanceName);
      }
    }
    return tagToInstances;
  }

  /**
   * Get the mapping between instance -> helix tags
   */
  public static Map<String, Set<String>> getInstanceToHelixTags(PinotHelixResourceManager pinotHelixResourceManager) {
    List<InstanceConfig> instanceConfigs = pinotHelixResourceManager.getAllHelixInstanceConfigs();
    Map<String, Set<String>> instanceToHelixTags = new HashMap<>();
    for (InstanceConfig instanceConfig : instanceConfigs) {
      String instanceName = instanceConfig.getInstanceName();
      List<String> tags = instanceConfig.getTags();
      instanceToHelixTags.computeIfAbsent(instanceName, k -> new HashSet<>()).addAll(tags);
    }
    return instanceToHelixTags;
  }

  /**
   * Get the mapping between helix tag -> QueryWorkloadConfig
   * 1. Get all QueryWorkloadConfigs from PinotHelixResourceManager
   * 2. For each QueryWorkloadConfig, get the node configs
   * 3. For each node config, check the propagation scheme type
   * 4. If the propagation type is TENANT, get the tenant names and add them to the helix tags
   * 5. If the propagation type is TABLE, get the table names and resolve them to helix tags
   */
  public static Map<String, Set<QueryWorkloadConfig>> getHelixTagToWorkloadConfigs(
      PinotHelixResourceManager pinotHelixResourceManager) {
    Map<String, Set<QueryWorkloadConfig>> helixTagsToWorkloadConfigs = new HashMap<>();
    List<QueryWorkloadConfig> queryWorkloadConfigs = pinotHelixResourceManager.getQueryWorkloadConfigs();
    if (queryWorkloadConfigs == null) {
      return helixTagsToWorkloadConfigs;
    }
    Map<String, Map<NodeConfig.Type, Set<String>>> tableToHelixTags = getTableToHelixTags(pinotHelixResourceManager);
    for (QueryWorkloadConfig queryWorkloadConfig : queryWorkloadConfigs) {
      Map<NodeConfig.Type, NodeConfig> nodeConfigs = queryWorkloadConfig.getNodeConfigs();
      nodeConfigs.forEach((nodeType, nodeConfig) -> {
        PropagationScheme.Type propagationType = nodeConfig.getPropagationScheme().getPropagationType();
        if (propagationType == PropagationScheme.Type.TENANT) {
          List<String> tenants = nodeConfig.getPropagationScheme().getValues();
          for (String tenant : tenants) {
            if (nodeType == NodeConfig.Type.LEAF_NODE) {
              if (TagNameUtils.isOfflineServerTag(tenant) || TagNameUtils.isRealtimeServerTag(tenant)) {
                helixTagsToWorkloadConfigs.computeIfAbsent(tenant, k -> new HashSet<>()).add(queryWorkloadConfig);
              } else {
                // Add both offline and realtime server tags
                helixTagsToWorkloadConfigs.computeIfAbsent(TagNameUtils.getOfflineTagForTenant(tenant),
                    k -> new HashSet<>()).add(queryWorkloadConfig);
                helixTagsToWorkloadConfigs.computeIfAbsent(TagNameUtils.getRealtimeTagForTenant(tenant),
                    k -> new HashSet<>()).add(queryWorkloadConfig);
              }
            } else if (nodeType == NodeConfig.Type.NON_LEAF_NODE) {
              tenant = TagNameUtils.getBrokerTagForTenant(tenant);
              helixTagsToWorkloadConfigs.computeIfAbsent(tenant, k -> new HashSet<>()).add(queryWorkloadConfig);
            }
          }
        } else if (propagationType == PropagationScheme.Type.TABLE) {
          List<String> tableNames = nodeConfig.getPropagationScheme().getValues();
          for (String tableName : tableNames) {
            TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
            List<String> tablesWithType = (tableType == null)
                    ? Arrays.asList(TableNameBuilder.OFFLINE.tableNameWithType(tableName),
                       TableNameBuilder.REALTIME.tableNameWithType(tableName))
                    : Collections.singletonList(tableName);
            for (String tableWithType : tablesWithType) {
              Map<NodeConfig.Type, Set<String>> tenants = tableToHelixTags.get(tableWithType);
              if (tenants != null) {
                Set<String> tenantNames = tenants.get(nodeType);
                if (tenantNames != null) {
                  for (String tenant : tenantNames) {
                    helixTagsToWorkloadConfigs.computeIfAbsent(tenant, k -> new HashSet<>()).add(queryWorkloadConfig);
                  }
                }
              }
            }
          }
        }
      });
    }
    return helixTagsToWorkloadConfigs;
  }
}
